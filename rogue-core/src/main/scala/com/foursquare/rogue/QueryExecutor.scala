// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{ReadPreference, WriteConcern}

trait QueryExecutor[MB] {
  def defaultReadPreference: ReadPreference
  def defaultWriteConcern: WriteConcern

  def count[M <: MB, Lim <: MaybeLimited, Sk <: MaybeSkipped](
      query: AbstractQuery[M, _, _, _, Lim, Sk, _]
  )(
      implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped
  ): Long

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
  ): Long

  def fetch[M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      readPreference: ReadPreference = defaultReadPreference
  ): Seq[R]

  def fetchOne[M <: MB, R, Lim <: MaybeLimited](
      query: AbstractQuery[M, R, _, _, Lim, _, _],
      readPreference: ReadPreference = defaultReadPreference
  )(
      implicit ev1: Lim =:= Unlimited
  ): Option[R]

  def foreach[M <: MB, R](
      query: AbstractQuery[M, R, _, _, _, _, _],
      readPreference: ReadPreference = defaultReadPreference
  )(
      f: R => Unit
  ): Unit

  def fetchBatch[M <: MB, R, T](
      query: AbstractQuery[M, R, _, _, _, _, _],
      batchSize: Int,
      readPreference: ReadPreference = defaultReadPreference
  )(
      f: List[R] => List[T]
  ): Seq[T]

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

  def findAndDeleteOne[M <: MB, R](
    query: AbstractQuery[M, R, _ <: MaybeOrdered, _ <: MaybeSelected, _ <: MaybeLimited, _ <: MaybeSkipped, _ <: MaybeHasOrClause],
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R]

  def explain[M <: MB](query: AbstractQuery[M, _, _, _, _, _, _]): String
}
