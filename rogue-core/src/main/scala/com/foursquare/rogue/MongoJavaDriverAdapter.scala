// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.index.UntypedMongoIndex
import com.foursquare.rogue.Rogue._
import com.foursquare.rogue.Iter._
import com.mongodb.{BasicDBObject, BasicDBObjectBuilder, CommandResult, DBCollection,
  DBCursor, DefaultDBDecoder, DBDecoderFactory, DBObject, ReadPreference, WriteConcern}
import scala.collection.mutable.ListBuffer

trait DBCollectionFactory[MB, RB] {
  def getDBCollection[M <: MB](query: Query[M, _, _]): DBCollection
  def getPrimaryDBCollection[M <: MB](query: Query[M, _, _]): DBCollection
  def getPrimaryDBCollection(record: RB): DBCollection
  def getInstanceName[M <: MB](query: Query[M, _, _]): String
  // A set of of indexes, which are ordered lists of field names
  def getIndexes[M <: MB](query: Query[M, _, _]): Option[List[UntypedMongoIndex]]
}

class MongoJavaDriverAdapter[MB, RB](
  dbCollectionFactory: DBCollectionFactory[MB, RB],
  decoderFactoryFunc: (MB) => DBDecoderFactory = (m: MB) => DefaultDBDecoder.FACTORY
) {

  import QueryHelpers._
  import MongoHelpers.MongoBuilder._

  private[rogue] def runCommand[M <: MB, T](descriptionFunc: () => String,
                                            query: Query[M, _, _])(f: => T): T = {
    // Use nanoTime instead of currentTimeMillis to time the query since
    // currentTimeMillis only has 10ms granularity on many systems.
    val start = System.nanoTime
    val instanceName: String = dbCollectionFactory.getInstanceName(query)
    // Note that it's expensive to call descriptionFunc, it does toString on the Query
    // the logger methods are call by name
    try {
      logger.onExecuteQuery(query, instanceName, descriptionFunc(), f)
    } catch {
      case e: Exception =>
        throw new RogueException("Mongo query on %s [%s] failed after %d ms".
                                 format(instanceName, descriptionFunc(),
          (System.nanoTime - start) / (1000 * 1000)), e)
    } finally {
      logger.log(query, instanceName, descriptionFunc(), (System.nanoTime - start) / (1000 * 1000))
    }
  }

  def count[M <: MB](query: Query[M, _, _], readPreference: Option[ReadPreference]): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val condition: DBObject = buildCondition(queryClause.condition)
    val descriptionFunc: () => String = () => buildConditionString("count", query.collectionName, queryClause)

    runCommand(descriptionFunc, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      coll.getCount(
        condition,
        null, // fields
        queryClause.lim.getOrElse(0).toLong,
        queryClause.sk.getOrElse(0).toLong,
        readPreference.getOrElse(coll.getReadPreference)
      )
    }
  }

  def countDistinct[M <: MB](query: Query[M, _, _],
                             key: String,
                             readPreference: Option[ReadPreference]): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)

    // TODO: fix this so it looks like the correct mongo shell command
    val descriptionFunc: () => String = () => buildConditionString("distinct", query.collectionName, queryClause)

    runCommand(descriptionFunc, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      coll.distinct(key, cnd, readPreference.getOrElse(coll.getReadPreference)).size()
    }
  }

  def distinct[M <: MB, R](query: Query[M, _, _],
                           key: String,
                           readPreference: Option[ReadPreference])
                          (f: R => Unit): Unit = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)

    // TODO: fix this so it looks like the correct mongo shell command
    val descriptionFunc: () => String = () => buildConditionString("distinct", query.collectionName, queryClause)

    runCommand(descriptionFunc, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      val rj = coll.distinct(key, cnd, readPreference.getOrElse(coll.getReadPreference))
      for (i <- 0 until rj.size) {
        f(rj.get(i).asInstanceOf[R])
      }
    }
  }

  def delete[M <: MB](query: Query[M, _, _],
                      writeConcern: WriteConcern): Unit = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val descriptionFunc: () => String = () => buildConditionString("remove", query.collectionName, queryClause)

    runCommand(descriptionFunc, queryClause) {
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

  def insertAll(record: RB, dbos: Seq[DBObject], writeConcern: WriteConcern): Unit = {
    val collection = dbCollectionFactory.getPrimaryDBCollection(record)
    collection.insert(QueryHelpers.list(dbos), writeConcern)
  }

  def remove(record: RB, dbo: DBObject, writeConcern: WriteConcern): Unit = {
    val collection = dbCollectionFactory.getPrimaryDBCollection(record)
    collection.remove(dbo, writeConcern)
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
      val descriptionFunc: () => String = {
        () => buildModifyString(mod.query.collectionName, modClause, upsert = upsert, multi = multi)
      }

      runCommand(descriptionFunc, modClause.query) {
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
      val descriptionFunc: () => String = {
        () => buildFindAndModifyString(mod.query.collectionName, modClause, returnNew, upsert, remove)
      }

      runCommand(descriptionFunc, modClause.query) {
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
      cursor.setDecoderFactory(decoderFactoryFunc(query.meta))
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

    doQuery("find", query, None, readPreference)( cursor => {
      cursor.setDecoderFactory(decoderFactoryFunc(query.meta))
      iter(cursor, initialState)
    })
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
      cursor.setDecoderFactory(decoderFactoryFunc(query.meta))
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

    val descriptionFunc: () => String = () => buildQueryString(operation, query.collectionName, queryClause)

    runCommand(descriptionFunc, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      val cursor = coll.find(cnd, sel)

      // Always apply batchSize *before* limit. If the caller passes a negative value to limit(),
      // the driver applies it instead to batchSize. (A negative batchSize means, return one batch
      // and close the cursor.) Then if we set batchSize, the negative "limit" is overwritten, and
      // the query executes without a limit.
      // http://api.mongodb.org/java/2.7.3/com/mongodb/DBCursor.html#limit(int)
      config.cursorBatchSize match {
        case None => {
          // Apply the batch size from the query
          batchSize.foreach(cursor.batchSize _)
        }
        case Some(None) => {
          // don't set batch size
        }
        case Some(Some(n)) => {
          // Use the configured default batch size
          cursor.batchSize(n)
        }
      }

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
    }
  }
}
