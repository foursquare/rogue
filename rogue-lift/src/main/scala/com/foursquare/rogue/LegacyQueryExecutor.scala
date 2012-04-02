// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.Rogue._
import com.foursquare.rogue.Rogue.Iter._
import com.mongodb.{BasicDBObjectBuilder, Bytes, DBCursor, DBObject, WriteConcern}
import net.liftweb.mongodb.MongoDB
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import scala.collection.mutable.ListBuffer

object LegacyQueryExecutor {

  import QueryHelpers._
  import MongoHelpers.MongoBuilder._

  private[rogue] def runCommand[T, M <: MongoRecord[_] with MongoMetaRecord[_]](description: => String,
                                   query: GenericBaseQuery[M, _])(f: => T): T = {
    val start = System.currentTimeMillis
    try {
      f
    } catch {
      case e: Exception =>
        throw new RogueException("Mongo query on %s [%s] failed after %d ms".
                                 format(query.meta.mongoIdentifier,
          description, System.currentTimeMillis - start), e)
    } finally {
      logger.log(query, description, System.currentTimeMillis - start)
    }
  }

  def condition[M <: MongoRecord[_] with MongoMetaRecord[_], T](operation: String,
                                        query: GenericBaseQuery[M, _])
                                       (f: DBObject => T): T = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause)
    val cnd = buildCondition(queryClause.condition)
    val description = buildConditionString(operation, query.meta.collectionName, queryClause)

    runCommand(description, queryClause) {
      f(cnd)
    }
  }

  def modify[M <: MongoRecord[_] with MongoMetaRecord[_], T](mod: BaseModifyQuery[M],
                                     upsert: Boolean,
                                     multi: Boolean,
                                     writeConcern: Option[WriteConcern] = None): Unit = {
    val modClause = transformer.transformModify(mod)
    validator.validateModify(modClause)
    if (!modClause.mod.clauses.isEmpty) {
      val q = buildCondition(modClause.query.condition)
      val m = buildModify(modClause.mod)
      lazy val description = buildModifyString(mod.query.meta.collectionName, modClause, upsert = upsert, multi = multi)

      runCommand(description, modClause.query) {
        MongoDB.useSession(mod.query/* TODO: .master*/.meta.mongoIdentifier) { db =>
          val coll = db.getCollection(mod.query/* TODO .master */.meta.collectionName)
          writeConcern match {
            case Some(theWriteConcern) => coll.update(q, m, upsert, multi, theWriteConcern)
            case None => coll.update(q, m, upsert, multi)
          }
        }
      }
    }
  }

  def findAndModify[M <: MongoRecord[_] with MongoMetaRecord[_], R](mod: BaseFindAndModifyQuery[M, R],
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
      lazy val description = buildFindAndModifyString(mod.query.meta.collectionName, modClause, returnNew, upsert, remove)

      runCommand(description, modClause.query) {
        MongoDB.useCollection(query.meta.mongoIdentifier, query.meta.collectionName) { coll =>
          val dbObj = coll.findAndModify(cnd, sel, ord.getOrElse(null), remove, m, returnNew, upsert)
          if (dbObj == null || dbObj.keySet.isEmpty) None
          else Option(dbObj).map(f)
        }
      }
    }
    else None
  }

  def query[M <: MongoRecord[_] with MongoMetaRecord[_]](operation: String,
                                 query: GenericBaseQuery[M, _],
                                 batchSize: Option[Int])
                                (f: DBObject => Unit): Unit = {
    doQuery(operation, query){cursor =>
      batchSize.foreach(cursor batchSize _)
      while (cursor.hasNext)
        f(cursor.next)
    }
  }

  def iterate[M <: MongoRecord[_] with MongoMetaRecord[_], R, S](
      operation: String,
      query: GenericBaseQuery[M, R],
      initialState: S,
      f: DBObject => R
  )(
      handler: (S, Event[R]) => Command[S]
  ): S = {
    def getObject(cursor: DBCursor): Either[R, Exception] = {
      try {
        Left(f(cursor.next))
      } catch {
        case e: Exception => Right(e)
      }
    }

    @scala.annotation.tailrec
    def iter(cursor: DBCursor, curState: S): S = {
      if (cursor.hasNext) {
        getObject(cursor) match {
          case Right(e) => handler(curState, Error(e)).state
          case Left(r) => handler(curState, Item(r)) match {
            case Continue(s) => iter(cursor, s)
            case Return(s) => s
          }
        }
      } else {
        handler(curState, EOF).state
      }
    }

    doQuery(operation, query)(cursor =>
      iter(cursor, initialState)
    )
  }

  def iterateBatch[M <: MongoRecord[_] with MongoMetaRecord[_], R, S](
      operation: String,
      query: GenericBaseQuery[M, R],
      batchSize: Int,
      initialState: S,
      f: DBObject => R
  )(
      handler: (S, Event[List[R]]) => Command[S]
  ): S = {
    val buf = new ListBuffer[R]

    def getBatch(cursor: DBCursor): Either[List[R], Exception] = {
      try {
        buf.clear()
        while (cursor.hasNext && buf.size < batchSize) {
          buf += f(cursor.next)
        }
        Left(buf.toList)
      } catch {
        case e: Exception => Right(e)
      }
    }

    @scala.annotation.tailrec
    def iter(cursor: DBCursor, curState: S): S = {
      if (cursor.hasNext) {
        getBatch(cursor) match {
          case Right(e) => handler(curState, Error(e)).state
          case Left(Nil) => handler(curState, EOF).state
          case Left(rs) => handler(curState, Item(rs)) match {
            case Continue(s) => iter(cursor, s)
            case Return(s) => s
          }
        }
      } else {
        handler(curState, EOF).state
      }
    }

    doQuery(operation, query)(cursor => {
      cursor.batchSize(batchSize)
      iter(cursor, initialState)
    })
  }


  def explain[M <: MongoRecord[_] with MongoMetaRecord[_]](operation: String,
                                   query: GenericBaseQuery[M, _]): String = {
    doQuery(operation, query){cursor =>
      cursor.explain.toString
    }
  }

  private[rogue] def doQuery[M <: MongoRecord[_] with MongoMetaRecord[_], T](
      operation: String,
      query: GenericBaseQuery[M, _]
  )(
      f: DBCursor => T
  ): T = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause)
    val cnd = buildCondition(queryClause.condition)
    val ord = queryClause.order.map(buildOrder)
    val sel = queryClause.select.map(buildSelect).getOrElse(BasicDBObjectBuilder.start.get)
    val hnt = queryClause.hint.map(buildHint)

    lazy val description = buildQueryString(operation, query.meta.collectionName, queryClause)

    runCommand(description, queryClause){
      MongoDB.useCollection(queryClause.meta.mongoIdentifier, queryClause.meta.collectionName) {
        coll =>
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
}
