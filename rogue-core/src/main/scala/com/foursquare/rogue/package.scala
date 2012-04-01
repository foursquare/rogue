// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare

package object rogue {
  type AbstractQuery[
      M,
      R,
      Ord <: MaybeOrdered,
      Sel <: MaybeSelected,
      Lim <: MaybeLimited,
      Sk <: MaybeSkipped,
      Or <: MaybeHasOrClause
  ] = BaseQuery[M, R, Ord, Sel, Lim, Sk, Or]

  type ModifyQuery[T] = BaseModifyQuery[T]
  type AbstractModifyQuery[M] = BaseModifyQuery[M]

  type AbstractFindAndModifyQuery[M, R] = BaseFindAndModifyQuery[M, R]

}
