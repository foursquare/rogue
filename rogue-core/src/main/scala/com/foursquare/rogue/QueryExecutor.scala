// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.{MongoModify, MongoSelect}
import com.mongodb.{DBObject, ReadPreference, WriteConcern}
import scala.collection.mutable.ListBuffer

trait RogueSerializer[R] {
  def fromDBObject(dbo: DBObject): R
}

abstract class QueryExecutor[MB](adapter: MongoJavaDriverAdapter[MB]) {
  def defaultReadPreference: ReadPreference
  def defaultWriteConcern: WriteConcern
  def optimizer: QueryOptimizer = new QueryOptimizer

  protected def serializer[M <: MB, R](
      meta: M,
      select: Option[MongoSelect[R]]
  ): RogueSerializer[R]

  def count[M <: MB, Lim <: MaybeLimited, Sk <: MaybeSkipped](
      query: AbstractQuery[M, _, _, _, Lim, Sk, _]
  )(
      implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped
  ): Long = {
    if (optimizer.isEmptyQuery(query)) {
      0L
    } else {
      adapter.count(query)
    }
  }

  def countDistinct[
      M <: MB,
      Sel <: MaybeSelected,
      Lim <: MaybeLimited,
      Sk <: MaybeSkipped,
      V
  ](
      query: AbstractQuery[M, _, _, Sel, Lim, Sk, _]
  )(
      field: M => QueryField[V, M]
  )(
      implicit
      // ev1: Sel =:= SelectedOne,
      ev2: Lim =:= Unlimited,
      ev3: Sk =:= Unskipped
  ): Long = {
    if (optimizer.isEmptyQuery(query)) {
      0L
    } else {
      adapter.countDistinct(query, field(query.meta).field.name)
    }
  }

  def fetch[M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      readPreference: ReadPreference = defaultReadPreference
  ): List[R] = {
    if (optimizer.isEmptyQuery(query)) {
      Nil
    } else {
      val s = serializer[M, R](query.meta, query.select)
      val rv = new ListBuffer[R]
      adapter.query(query, None)(dbo => rv += s.fromDBObject(dbo))
      rv.toList
    }
  }

  def fetchOne[M <: MB, R, Lim <: MaybeLimited](
      query: AbstractQuery[M, R, _, _, Lim, _, _],
      readPreference: ReadPreference = defaultReadPreference
  )(
      implicit ev1: Lim =:= Unlimited
  ): Option[R] = fetch(query.limit(1), readPreference).headOption

  def foreach[M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      readPreference: ReadPreference = defaultReadPreference
  )(
      f: R => Unit
  ): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      val s = serializer[M, R](query.meta, query.select)
      adapter.query(query, None)(dbo => f(s.fromDBObject(dbo)))
    }
  }

  private def drainBuffer[A, B](
      from: ListBuffer[A],
      to: ListBuffer[B],
      f: List[A] => List[B],
      size: Int
  ): Unit = {
    // ListBuffer#length is O(1) vs ListBuffer#size is O(N) (true in 2.9.x, fixed in 2.10.x)
    if (from.length >= size) {
      to ++= f(from.toList)
      from.clear
    }
  }

  def fetchBatch[M <: MB, R, T](
      query: AbstractQuery[M, R, _, _, _, _, _],
      batchSize: Int,
      readPreference: ReadPreference = defaultReadPreference
  )(
      f: List[R] => List[T]
  ): List[T] = {
    if (optimizer.isEmptyQuery(query)) {
      Nil
    } else {
      val s = serializer[M, R](query.meta, query.select)
      val rv = new ListBuffer[T]
      val buf = new ListBuffer[R]

      adapter.query(query, Some(batchSize)) { dbo =>
        buf += s.fromDBObject(dbo)
        drainBuffer(buf, rv, f, batchSize)
      }
      drainBuffer(buf, rv, f, 1)

      rv.toList
    }
  }

  def bulkDelete_!![M <: MB, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped](
      query: AbstractQuery[M, _, _, Sel, Lim, Sk, _],
      writeConcern: WriteConcern = defaultWriteConcern
  )(
      implicit
      ev1: Sel <:< Unselected,
      ev2: Lim =:= Unlimited,
      ev3: Sk =:= Unskipped
  ): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.delete(query, writeConcern)
    }
  }

  def updateOne[M <: MB](
      query: AbstractModifyQuery[M],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.modify(query, upsert = false, multi = false, writeConcern = writeConcern)
    }
  }

  def upsertOne[M <: MB](
      query: AbstractModifyQuery[M],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.modify(query, upsert = true, multi = false, writeConcern = writeConcern)
    }
  }

  def updateMulti[M <: MB](
      query: AbstractModifyQuery[M],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.modify(query, upsert = false, multi = true, writeConcern = writeConcern)
    }
  }

  def findAndUpdateOne[M <: MB, R](
    query: AbstractFindAndModifyQuery[M, R],
    returnNew: Boolean = false,
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R] = {
    if (optimizer.isEmptyQuery(query)) {
      None
    } else {
      val s = serializer[M, R](query.query.meta, query.query.select)
      adapter.findAndModify(query, returnNew, upsert=false, remove=false)(s.fromDBObject _)
    }
  }

  def findAndUpsertOne[M <: MB, R](
    query: AbstractFindAndModifyQuery[M, R],
    returnNew: Boolean = false,
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R] = {
    if (optimizer.isEmptyQuery(query)) {
      None
    } else {
      val s = serializer[M, R](query.query.meta, query.query.select)
      adapter.findAndModify(query, returnNew, upsert=true, remove=false)(s.fromDBObject _)
    }
  }

  def findAndDeleteOne[M <: MB, R](
    query: AbstractQuery[M, R, _ <: MaybeOrdered, _ <: MaybeSelected, _ <: MaybeLimited, _ <: MaybeSkipped, _ <: MaybeHasOrClause],
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R] = {
    if (optimizer.isEmptyQuery(query)) {
      None
    } else {
      val s = serializer[M, R](query.meta, query.select)
      val mod = BaseFindAndModifyQuery(query, MongoModify(Nil))
      adapter.findAndModify(mod, returnNew=false, upsert=false, remove=true)(s.fromDBObject _)
    }
  }

  def explain[M <: MB](query: AbstractQuery[M, _, _, _, _, _, _]): String = {
    adapter.explain(query)
  }

  def iterate[S, M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      state: S
  )(
      handler: (S, Rogue.Iter.Event[R]) => Rogue.Iter.Command[S]
  ): S = {
    if (optimizer.isEmptyQuery(query)) {
      handler(state, Rogue.Iter.EOF).state
    } else {
      val s = serializer[M, R](query.meta, query.select)
      adapter.iterate(query, state, s.fromDBObject _)(handler)
    }
  }

  def iterateBatch[S, M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      batchSize: Int,
      state: S
  )(
      handler: (S, Rogue.Iter.Event[List[R]]) => Rogue.Iter.Command[S]
  ): S = {
    if (optimizer.isEmptyQuery(query)) {
      handler(state, Rogue.Iter.EOF).state
    } else {
      val s = serializer[M, R](query.meta, query.select)
      adapter.iterateBatch(query, batchSize, state, s.fromDBObject _)(handler)
    }
  }
}
