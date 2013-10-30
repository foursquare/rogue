// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.index.UntypedMongoIndex
import com.foursquare.rogue.Rogue._
import com.foursquare.rogue.Iter._
import com.mongodb.{BasicDBObject, BasicDBObjectBuilder, CommandResult, DBCollection,
  DBCursor, DBObject, ReadPreference, WriteConcern}
import scala.collection.mutable.ListBuffer

trait DBCollectionFactory[MB, RB] {
  def getDBCollection[M <: MB](query: Query[M, _, _]): DBCollection
  def getPrimaryDBCollection[M <: MB](query: Query[M, _, _]): DBCollection
  def getPrimaryDBCollection(record: RB): DBCollection
  def getInstanceName[M <: MB](query: Query[M, _, _]): String
  // A set of of indexes, which are ordered lists of field names
  def getIndexes[M <: MB](query: Query[M, _, _]): Option[List[UntypedMongoIndex]]
}

class MongoJavaDriverAdapter[MB, RB](dbCollectionFactory: DBCollectionFactory[MB, RB]) {

  import QueryHelpers._
  import MongoHelpers.MongoBuilder._

  private[rogue] def runCommand[M <: MB, T](description: => String,
                                            query: Query[M, _, _])(f: => T): T = {
    // Use nanoTime instead of currentTimeMillis to time the query since
    // currentTimeMillis only has 10ms granularity on many systems.
    val start = System.nanoTime
    val instanceName: String = dbCollectionFactory.getInstanceName(query)
    try {
      logger.onExecuteQuery(query, instanceName, description, f)
    } catch {
      case e: Exception =>
        throw new RogueException("Mongo query on %s [%s] failed after %d ms".
                                 format(instanceName, description,
          (System.nanoTime - start) / (1000 * 1000)), e)
    } finally {
      logger.log(query, instanceName, description, (System.nanoTime - start) / (1000 * 1000))
    }
  }

  def count[M <: MB](query: Query[M, _, _]): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val condition: DBObject = buildCondition(queryClause.condition)
    val description: String = buildConditionString("count", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      // there's a bug in mongo-java-driver 2.7.0 - 2.8.0 that
      // doesn't set the ReadPreference into commands correctly
      val coll = dbCollectionFactory.getDBCollection(query)
      val db = coll.getDB
      val cmd = new BasicDBObject()
      cmd.put("count", query.collectionName)
      cmd.put("query", condition)

      queryClause.lim.filter(_ > 0).foreach( cmd.put("limit", _) )
      queryClause.sk.filter(_ > 0).foreach( cmd.put("skip", _) )

      // 4sq dynamically throttles ReadPreference via an override of
      // DBCursor creation.  We don't want to override for the whole
      // DBCollection because those are cached for the life of the DB
      val result: CommandResult = db.command(cmd, coll.getOptions, coll.find().getReadPreference)
      if (!result.ok) {
        result.getErrorMessage match {
          // pretend count is zero craziness from the mongo-java-driver
          case "ns does not exist" | "ns missing" => 0L
          case _ =>
            result.throwOnError()
            0L
        }
      } else {
        result.getLong("n")
      }
    }
  }

  def countDistinct[M <: MB](query: Query[M, _, _],
                             key: String): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)

    // TODO: fix this so it looks like the correct mongo shell command
    val description = buildConditionString("distinct", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      coll.distinct(key, cnd).size()
    }
  }

  def distinct[M <: MB, R](query: Query[M, _, _],
                           key: String): List[R] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)

    // TODO: fix this so it looks like the correct mongo shell command
    val description = buildConditionString("distinct", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      val rv = new ListBuffer[R]
      val rj = coll.distinct(key, cnd)
      for (i <- 0 until rj.size) rv += rj.get(i).asInstanceOf[R]
      rv.toList
    }
  }

  def delete[M <: MB](query: Query[M, _, _],
                      writeConcern: WriteConcern): Unit = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val description = buildConditionString("remove", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getPrimaryDBCollection(query)
      coll.remove(cnd, writeConcern)
    }
  }

  def save(record: RB, dbo: DBObject, writeConcern: WriteConcern): Unit = {
    val collection = dbCollectionFactory.getPrimaryDBCollection(record)
    collection.save(dbo, writeConcern)
  }

  def insert(record: RB, dbo: DBObject, writeConcern: WriteConcern): Unit = {
    val collection = dbCollectionFactory.getPrimaryDBCollection(record)
    collection.insert(dbo, writeConcern)
  }

  def modify[M <: MB](mod: ModifyQuery[M, _],
                      upsert: Boolean,
                      multi: Boolean,
                      writeConcern: WriteConcern): Unit = {
    val modClause = transformer.transformModify(mod)
    validator.validateModify(modClause, dbCollectionFactory.getIndexes(modClause.query))
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

  def findAndModify[M <: MB, R](mod: FindAndModifyQuery[M, R],
                                returnNew: Boolean,
                                upsert: Boolean,
                                remove: Boolean)
                               (f: DBObject => R): Option[R] = {
    val modClause = transformer.transformFindAndModify(mod)
    validator.validateFindAndModify(modClause, dbCollectionFactory.getIndexes(modClause.query))
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

  def query[M <: MB](query: Query[M, _, _],
                     batchSize: Option[Int],
                     readPreference: Option[ReadPreference])
                    (f: DBObject => Unit): Unit = {
    doQuery("find", query, batchSize, readPreference){cursor =>
      while (cursor.hasNext)
        f(cursor.next)
    }
  }

  def iterate[M <: MB, R, S](query: Query[M, R, _],
                             initialState: S,
                             f: DBObject => R,
                             readPreference: Option[ReadPreference] = None)
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

    doQuery("find", query, None, readPreference)(cursor =>
      iter(cursor, initialState)
    )
  }

  def iterateBatch[M <: MB, R, S](query: Query[M, R, _],
                                  batchSize: Int,
                                  initialState: S,
                                  f: DBObject => R,
                                  readPreference: Option[ReadPreference] = None)
                                 (handler: (S, Event[List[R]]) => Command[S]): S = {
    val buf = new ListBuffer[R]

    def getBatch(cursor: DBCursor): Either[Exception, List[R]] = {
      try {
        buf.clear()
        // ListBuffer#length is O(1) vs ListBuffer#size is O(N) (true in 2.9.x, fixed in 2.10.x)
        while (cursor.hasNext && buf.length < batchSize) {
          buf += f(cursor.next)
        }
        Right(buf.toList)
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

    doQuery("find", query, Some(batchSize), readPreference)(cursor => {
      iter(cursor, initialState)
    })
  }


  def explain[M <: MB](query: Query[M, _, _]): String = {
    doQuery("find", query, None, None){cursor =>
      cursor.explain.toString
    }
  }

  private def doQuery[M <: MB, T](
      operation: String,
      query: Query[M, _, _],
      batchSize: Option[Int],
      readPreference: Option[ReadPreference]
  )(
      f: DBCursor => T
  ): T = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val ord = queryClause.order.map(buildOrder)
    val sel = queryClause.select.map(buildSelect).getOrElse(BasicDBObjectBuilder.start.get)
    val hnt = queryClause.hint.map(buildHint)

    lazy val description = buildQueryString(operation, query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      try {
        val cursor = coll.find(cnd, sel)
        // Always apply batchSize *before* limit. If the caller passes a negative value to limit(),
        // the driver applies it instead to batchSize. (A negative batchSize means, return one batch
        // and close the cursor.) Then if we set batchSize, the negative "limit" is overwritten, and
        // the query executes without a limit.
        // http://api.mongodb.org/java/2.7.3/com/mongodb/DBCursor.html#limit(int)
        batchSize.foreach(cursor batchSize _)
        queryClause.lim.foreach(cursor.limit _)
        queryClause.sk.foreach(cursor.skip _)
        ord.foreach(cursor.sort _)
        readPreference.orElse(queryClause.readPreference).foreach(cursor.setReadPreference _)
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
