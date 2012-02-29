// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.mongodb.WriteConcern
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}

case class LiftQuery[
    M <: MongoRecord[_] with MongoMetaRecord[_],
    R,
    Ord <: MaybeOrdered,
    Sel <: MaybeSelected,
    Lim <: MaybeLimited,
    Sk <: MaybeSkipped,
    Or <: MaybeHasOrClause
](
    query: BaseQuery[M, R, Ord, Sel, Lim, Sk, Or],
    db: LiftQueryExecutor
) {
  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    db.count(query)

  def countDistinct[V](field: M => QueryField[V, M])
                       (implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    db.countDistinct(query)(field)

  def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean =
    db.fetch(query.copy(select = Some(MongoSelect[Null, M](Nil, _ => null))).limit(1)).size > 0

  def foreach(f: R => Unit): Unit =
    db.foreach(query)(f)

  def fetch(): List[R] =
    db.fetch(query)

  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] =
    db.fetch(query.limit(limit))

  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = 
    db.fetchBatch(query, batchSize)(f).toList

  def get()(implicit ev: Lim =:= Unlimited): Option[R] =
    db.fetchOne(query)

  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    sys.error("TODO")
    // new BasePaginatedQuery(this.copy(), countPerPage)
  }

  // Always do modifications against master (not meta, which could point to slave)
  def bulkDelete_!!!()(implicit ev1: Sel <:< Unselected,
                               ev2: Lim =:= Unlimited,
                               ev3: Sk =:= Unskipped): Unit =
    db.bulkDelete_!!(query)

  def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel <:< Unselected,
                                                            ev2: Lim =:= Unlimited,
                                                            ev3: Sk =:= Unskipped): Unit =
    db.bulkDelete_!!(query, concern)

  def findAndDeleteOne(): Option[R] =
    db.findAndDeleteOne(query)

  def explain(): String =
    db.explain(query)
}


case class LiftModifyQuery[M <: MongoRecord[_] with MongoMetaRecord[_]](
    query: BaseModifyQuery[M],
    db: LiftQueryExecutor
) {
  // These methods always do modifications against master (not query.meta, which could point to a slave).
  def updateMulti(): Unit =
    db.updateMulti(query)

  def updateOne(): Unit =
    db.updateOne(query)

  def upsertOne(): Unit =
    db.upsertOne(query)

  def updateMulti(writeConcern: WriteConcern): Unit =
    db.updateMulti(query, writeConcern)

  def updateOne(writeConcern: WriteConcern): Unit =
    db.updateOne(query, writeConcern)

  def upsertOne(writeConcern: WriteConcern): Unit =
    db.upsertOne(query, writeConcern)
}

case class LiftFindAndModifyQuery[M <: MongoRecord[_] with MongoMetaRecord[_], R](
    query: BaseFindAndModifyQuery[M, R],
    db: LiftQueryExecutor
) {
  // Always do modifications against master (not query.meta, which could point to slave)
  def updateOne(returnNew: Boolean = false): Option[R] =
    db.findAndUpdateOne(query, returnNew)

  def upsertOne(returnNew: Boolean = false): Option[R] =
    db.findAndUpsertOne(query, returnNew)
}
