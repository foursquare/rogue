// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{ReadPreference, WriteConcern}

trait QueryExecutor[MB] {
  def defaultReadPreference: ReadPreference
  def defaultWriteConcern: WriteConcern

  /**
   * Gets the size of the query result. This should only be called on queries that do not
   * have limits or skips.
   */
  def count[M <: MB, Lim <: MaybeLimited, Sk <: MaybeSkipped](
      query: AbstractQuery[M, _, _, _, Lim, Sk, _]
  )(
      implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped
  ): Long

  /**
   * Returns the number of distinct values returned by a query. The query must not have
   * limit or skip clauses.
   */
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
      ev2: Lim =:= Unlimited,
      ev3: Sk =:= Unskipped
  ): Long

  /**
   * Execute the query, returning all of the records that match the query.
   * @return a list containing the records that match the query
   */
  def fetch[M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      readPreference: ReadPreference = defaultReadPreference
  ): Seq[R]

  /**
   * Fetches the first record that matches the query. The query must not contain a "limited" clause.
   * @return an option record containing either the first result that matches the
   *     query, or None if there are no records that match.
   */
  def fetchOne[M <: MB, R, Lim <: MaybeLimited](
      query: AbstractQuery[M, R, _, _, Lim, _, _],
      readPreference: ReadPreference = defaultReadPreference
  )(
      implicit ev1: Lim =:= Unlimited
  ): Option[R]

  /**
   * Executes a function on each record value returned by a query.
   * @param f a function to be invoked on each fetched record.
   * @return nothing.
   */
  def foreach[M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      readPreference: ReadPreference = defaultReadPreference
  )(
      f: R => Unit
  ): Unit

  /**
   * fetch a batch of results, and execute a function on each element of the list.
   * @param f the function to invoke on the records that match the query.
   * @return a sequence containing the results of invoking the function on each record.
   */
  def fetchBatch[M <: MB, R, T](
      query: AbstractQuery[M, R, _, _, _, _, _],
      batchSize: Int,
      readPreference: ReadPreference = defaultReadPreference
  )(
      f: List[R] => List[T]
  ): Seq[T]

  /**
   * Delete all of the recurds that match the query. The query must not contain any "skip",
   * "limit", or "select" clauses.
   */
  def bulkDelete_!![M <: MB, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped](
      query: AbstractQuery[M, _, _, Sel, Lim, Sk, _],
      writeConcern: WriteConcern = defaultWriteConcern
  )(
      implicit
      ev1: Sel <:< Unselected,
      ev2: Lim =:= Unlimited,
      ev3: Sk =:= Unskipped
  ): Unit

  def updateOne[M <: MB](
      query: AbstractModifyQuery[M],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit

  def upsertOne[M <: MB](
      query: AbstractModifyQuery[M],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit

  def updateMulti[M <: MB](
      query: AbstractModifyQuery[M],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit

  def findAndUpdateOne[M <: MB, R](
    query: AbstractFindAndModifyQuery[M, R],
    returnNew: Boolean = false,
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R]

  def findAndUpsertOne[M <: MB, R](
    query: AbstractFindAndModifyQuery[M, R],
    returnNew: Boolean = false,
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R]

  /**
   * Finds the first record that matches the query (if any), fetches it, and then deletes it.
   * A copy of the deleted record is returned to the caller.
   */
  def findAndDeleteOne[M <: MB, R](
    query: AbstractQuery[M, R, _ <: MaybeOrdered, _ <: MaybeSelected, _ <: MaybeLimited, _ <: MaybeSkipped, _ <: MaybeHasOrClause],
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R]

  /**
   * Return a string containing details about how the query would be executed in mongo.
   * In particular, this is useful for finding out what indexes will be used by the query.
   */
  def explain[M <: MB](query: AbstractQuery[M, _, _, _, _, _, _]): String

  def iterate[S, M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      state: S
  )(
      handler: (S, Rogue.Iter.Event[R]) => Rogue.Iter.Command[S]
  ): S

  def iterateBatch[S, M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      batchSize: Int,
      state: S
  )(
      handler: (S, Rogue.Iter.Event[List[R]]) => Rogue.Iter.Command[S]
  ): S
}
