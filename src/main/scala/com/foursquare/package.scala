package com.foursquare

import net.liftweb.mongodb.record.MongoRecord

package object rogue {
  type AbstractQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped, Or <: MaybeHasOrClause] =
    BaseQuery[M, R, Ord, Sel, Lim, Sk, Or]
  type AbstractModifyQuery[T <: MongoRecord[T]] = BaseModifyQuery[T]
  type AbstractFindAndModifyQuery[T <: MongoRecord[T], R] = BaseFindAndModifyQuery[T, R]
}
