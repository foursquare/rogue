package com.foursquare.rogue

// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.mongodb.WriteConcern

case class ExecutableQuery[
    MB,
    M <: MB,
    R,
    Ord <: MaybeOrdered,
    Sel <: MaybeSelected,
    Lim <: MaybeLimited,
    Sk <: MaybeSkipped,
    Or <: MaybeHasOrClause
](
    query: AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or],
    db: QueryExecutor[MB]
) {

  /**
   * Gets the size of the query result. This should only be called on queries that do not
   * have limits or skips.
   */
  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    db.count(query)

  /**
   * Returns the number of distinct values returned by a query. The query must not have
   * limit or skip clauses.
   */
  def countDistinct[V](field: M => QueryField[V, _])
                      (implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    db.countDistinct(query)(field.asInstanceOf[M => QueryField[V, M]])

  /**
   * Checks if there are any records that match this query.
   */
  def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean =
    db.fetch(query.copy(select = Some(MongoSelect[Null](Nil, _ => null))).limit(1)).size > 0

  /**
   * Executes a function on each record value returned by a query.
   * @param f a function to be invoked on each fetched record.
   * @return nothing.
   */
  def foreach(f: R => Unit): Unit =
    db.foreach(query)(f)

  /**
   * Execute the query, returning all of the records that match the query.
   * @return a list containing the records that match the query
   */
  def fetch(): List[R] =
    db.fetch(query)

  /**
   * Execute a query, returning no more than a specified number of result records. The
   * query must not have a limit clause.
   * @param limit the maximum number of records to return.
   */
  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] =
    db.fetch(query.limit(limit))

  /**
   * fetch a batch of results, and execute a function on each element of the list.
   * @param f the function to invoke on the records that match the query.
   * @return a list containing the results of invoking the function on each record.
   */
  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] =
    db.fetchBatch(query, batchSize)(f).toList

  /**
   * Fetches the first record that matches the query. The query must not contain a "limited" clause.
   * @return an option record containing either the first result that matches the
   *         query, or None if there are no records that match.
   */
  def get()(implicit ev: Lim =:= Unlimited): Option[R] =
    db.fetchOne(query)

  /**
   * Fetches the records that match the query in paginated form. The query must not contain
   * a "limit" clause.
   * @param countPerPage the number of records to be contained in each page of the result.
   */
  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    val q = query.asInstanceOf[AbstractQuery[M, R, Ord, Sel, Unlimited, Unskipped, Or]]
    new BasePaginatedQuery[MB, M, R](q, db, countPerPage)
  }

  /**
   * Delete all of the records that match the query. The query must not contain any "skip",
   * "limit", or "select" clauses. Sends the delete operation to mongo, and returns - does
   * <em>not</em> wait for the delete to be finished.
   */
  def bulkDelete_!!!()(implicit ev1: Sel <:< Unselected,
                       ev2: Lim =:= Unlimited,
                       ev3: Sk =:= Unskipped): Unit =
    db.bulkDelete_!!(query)

  /**
   * Delete all of the records that match the query. The query must not contain any "skip",
   * "limit", or "select" clauses. Sends the delete operation to mongo, and waits for the
   * delete operation to complete before returning to the caller.
   */
  def bulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel <:< Unselected,
                                           ev2: Lim =:= Unlimited,
                                           ev3: Sk =:= Unskipped): Unit =
    db.bulkDelete_!!(query, concern)

  /**
   * Finds the first record that matches the query (if any), fetches it, and then deletes it.
   * A copy of the deleted record is returned to the caller.
   */
  def findAndDeleteOne(): Option[R] =
    db.findAndDeleteOne(query)

  /**
   * Return a string containing details about how the query would be executed in mongo.
   * In particular, this is useful for finding out what indexes will be used by the query.
   */
  def explain(): String =
    db.explain(query)

  def iterate[S](state: S)(handler: (S, Rogue.Iter.Event[R]) => Rogue.Iter.Command[S]): S =
    db.iterate(query, state)(handler)

  def iterateBatch[S, CollType <: Traversable[R]](batchSize: Int, state: S)(handler: (S, Rogue.Iter.Event[Traversable[R]]) => Rogue.Iter.Command[S]): S =
    db.iterateBatch(query, batchSize, state)(handler)
}

case class ExecutableModifyQuery[MB, M <: MB](query: AbstractModifyQuery[M],
                                              db: QueryExecutor[MB]) {
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

case class ExecutableFindAndModifyQuery[MB, M <: MB, R](
    query: AbstractFindAndModifyQuery[M, R],
    db: QueryExecutor[MB]
) {
  def updateOne(returnNew: Boolean = false): Option[R] =
    db.findAndUpdateOne(query, returnNew)

  def upsertOne(returnNew: Boolean = false): Option[R] =
    db.findAndUpsertOne(query, returnNew)
}

class BasePaginatedQuery[MB, M <: MB, R](
    q: AbstractQuery[M, R, _, _, Unlimited, Unskipped, _],
    db: QueryExecutor[MB],
    val countPerPage: Int,
    val pageNum: Int = 1
) {
  def copy() = new BasePaginatedQuery(q, db, countPerPage, pageNum)

  def setPage(p: Int) = if (p == pageNum) this else new BasePaginatedQuery(q, db, countPerPage, p)

  def setCountPerPage(c: Int) = if (c == countPerPage) this else new BasePaginatedQuery(q, db, c, pageNum)

  lazy val countAll: Long = db.count(q)

  def fetch(): List[R] = db.fetch(q.skip(countPerPage * (pageNum - 1)).limit(countPerPage))

  def numPages = math.ceil(countAll.toDouble / countPerPage.toDouble).toInt max 1
}
