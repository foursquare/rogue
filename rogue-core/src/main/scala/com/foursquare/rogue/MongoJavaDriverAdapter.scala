// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.Rogue._
import com.foursquare.rogue.Rogue.Iter._
import com.mongodb.{BasicDBObjectBuilder, Bytes, DBCollection, DBCursor, DBObject, WriteConcern}
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

trait DBCollectionFactory[MB] {
  def getDBCollection[M <: MB](query: GenericQuery[M, _]): DBCollection
  def getPrimaryDBCollection[M <: MB](query: GenericQuery[M, _]): DBCollection
  def getInstanceName[M <: MB](query: GenericQuery[M, _]): String
}

class MongoJavaDriverAdapter[MB](dbCollectionFactory: DBCollectionFactory[MB]) {

  import QueryHelpers._
  import MongoHelpers.MongoBuilder._

  private[rogue] def runCommand[M <: MB, T](description: => String,
                                            query: GenericQuery[M, _])(f: => T): T = {
    // Use nanoTime instead of currentTimeMillis to time the query since
    // currentTimeMillis only has 10ms granularity on many systems.
    val start = System.nanoTime
    try {
      f
    } catch {
      case e: Exception =>
        throw new RogueException("Mongo query on %s [%s] failed after %d ms".
                                 format(dbCollectionFactory.getInstanceName(query),
          description, (System.nanoTime - start) / (1000 * 1000)), e)
    } finally {
      logger.log(query, description, (System.nanoTime - start) / (1000 * 1000))
    }
  }

  def count[M <: MB](query: GenericQuery[M, _]): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause)
    val cnd = buildCondition(queryClause.condition)
    val description = buildConditionString("count", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      val cursor = coll.find(cnd)
      cursor.count()
    }
  }

  def countDistinct[M <: MB](query: GenericQuery[M, _],
                             key: String): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause)
    val cnd = buildCondition(queryClause.condition)

    // TODO: fix this so it looks like the correct mongo shell command
    val description = buildConditionString("distinct", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      coll.distinct(key, cnd).size()
    }
  }

  def delete[M <: MB](query: GenericQuery[M, _],
                      writeConcern: WriteConcern): Unit = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause)
    val cnd = buildCondition(queryClause.condition)
    val description = buildConditionString("remove", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getPrimaryDBCollection(query)
      coll.remove(cnd, writeConcern)
    }
  }

  def modify[M <: MB](mod: AbstractModifyQuery[M],
                      upsert: Boolean,
                      multi: Boolean,
                      writeConcern: WriteConcern): Unit = {
    val modClause = transformer.transformModify(mod)
    validator.validateModify(modClause)
    if (!modClause.mod.clauses.isEmpty) {
      val q = buildCondition(modClause.query.condition)
      val m = buildModify(modClause.mod)
      lazy val description = buildModifyString(mod.query.collectionName, modClause, upsert = upsert, multi = multi)

      runCommand(description, modClause.query) {
        val coll = dbCollectionFactory.getPrimaryDBCollection(modClause.query)
        coll.update(q, m, upsert, multi, writeConcern)
      }
    }
  }

  def findAndModify[M <: MB, R](mod: AbstractFindAndModifyQuery[M, R],
                                returnNew: Boolean,
                                upsert: Boolean,
                                remove: Boolean)
                               (f: DBObject => R): Option[R] = {
    val modClause = transformer.transformFindAndModify(mod)
    validator.validateFindAndModify(modClause)
    if (!modClause.mod.clauses.isEmpty || remove) {
      val query = modClause.query
      val cnd = buildCondition(query.condition)
      val ord = query.order.map(buildOrder)
      val sel = query.select.map(buildSelect).getOrElse(BasicDBObjectBuilder.start.get)
      val m = buildModify(modClause.mod)
      lazy val description = buildFindAndModifyString(mod.query.collectionName, modClause, returnNew, upsert, remove)

      runCommand(description, modClause.query) {
        val coll = dbCollectionFactory.getPrimaryDBCollection(query)
        val dbObj = coll.findAndModify(cnd, sel, ord.getOrElse(null), remove, m, returnNew, upsert)
        if (dbObj == null || dbObj.keySet.isEmpty) None
        else Option(dbObj).map(f)
      }
    }
    else None
  }

  def query[M <: MB](query: GenericQuery[M, _],
                     batchSize: Option[Int])
                    (f: DBObject => Unit): Unit = {
    doQuery("find", query){cursor =>
      batchSize.foreach(cursor batchSize _)
      while (cursor.hasNext)
        f(cursor.next)
    }
  }

  def iterate[M <: MB, R, S](query: GenericQuery[M, R],
                             initialState: S,
                             f: DBObject => R)
                            (handler: (S, Event[R]) => Command[S]): S = {
    def getObject(cursor: DBCursor): Either[Exception, R] = {
      try {
        Right(f(cursor.next))
      } catch {
        case e: Exception => Left(e)
      }
    }

    @scala.annotation.tailrec
    def iter(cursor: DBCursor, curState: S): S = {
      if (cursor.hasNext) {
        getObject(cursor) match {
          case Left(e) => handler(curState, Error(e)).state
          case Right(r) => handler(curState, Item(r)) match {
            case Continue(s) => iter(cursor, s)
            case Return(s) => s
          }
        }
      } else {
        handler(curState, EOF).state
      }
    }

    doQuery("find", query)(cursor =>
      iter(cursor, initialState)
    )
  }

  def iterateBatch[M <: MB, R, S, CollType <: Traversable[R]](
                                  query: GenericQuery[M, R],
                                  batchSize: Int,
                                  initialState: S,
                                  f: DBObject => R)
                                 (handler: (S, Event[CollType]) => Command[S])
                                 (implicit cbf: CanBuildFrom[Traversable[_], R, CollType]): S = {

    def getBatch(cursor: DBCursor): Either[Exception, CollType] = {
      try {
        val buf = cbf()
        var count = 0
        while (cursor.hasNext && count < batchSize) {
          buf += f(cursor.next)
          count += 1
        }
        Right(buf.result())
      } catch {
        case e: Exception => Left(e)
      }
    }

    @scala.annotation.tailrec
    def iter(cursor: DBCursor, curState: S): S = {
      if (cursor.hasNext) {
        getBatch(cursor) match {
          case Left(e) => handler(curState, Error(e)).state
          case Right(Nil) => handler(curState, EOF).state
          case Right(rs) => handler(curState, Item(rs)) match {
            case Continue(s) => iter(cursor, s)
            case Return(s) => s
          }
        }
      } else {
        handler(curState, EOF).state
      }
    }

    doQuery("find", query)(cursor => {
      cursor.batchSize(batchSize)
      iter(cursor, initialState)
    })
  }


  def explain[M <: MB](query: GenericQuery[M, _]): String = {
    doQuery("find", query){cursor =>
      cursor.explain.toString
    }
  }

  private def doQuery[M <: MB, T](
      operation: String,
      query: GenericQuery[M, _]
  )(
      f: DBCursor => T
  ): T = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause)
    val cnd = buildCondition(queryClause.condition)
    val ord = queryClause.order.map(buildOrder)
    val sel = queryClause.select.map(buildSelect).getOrElse(BasicDBObjectBuilder.start.get)
    val hnt = queryClause.hint.map(buildHint)

    lazy val description = buildQueryString(operation, query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      try {
        val cursor = coll.find(cnd, sel)
        queryClause.lim.foreach(cursor.limit _)
        queryClause.sk.foreach(cursor.skip _)
        ord.foreach(cursor.sort _)
        queryClause.slaveOk.foreach(so => {
          if (so) {
            // Use bitwise-or to add in slave-ok
            cursor.setOptions(cursor.getOptions | Bytes.QUERYOPTION_SLAVEOK)
          } else {
            // Remove slave-ok from options
            cursor.setOptions(cursor.getOptions & ~Bytes.QUERYOPTION_SLAVEOK)
          }
        })
        queryClause.maxScan.foreach(cursor addSpecial("$maxScan", _))
        queryClause.comment.foreach(cursor addSpecial("$comment", _))
        hnt.foreach(cursor hint _)
        val ret = f(cursor)
        cursor.close()
        ret
      } catch {
        case e: Exception =>
          throw new RogueException("Mongo query on %s [%s] failed".format(
            coll.getDB().getMongo().toString(), description), e)
      }
    }
  }
}
