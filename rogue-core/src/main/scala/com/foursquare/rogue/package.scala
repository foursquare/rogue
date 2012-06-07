// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare

package object rogue {
  type AbstractQuery[M, R, +State] = BaseQuery[M, R, State]

  type ModifyQuery[T] = BaseModifyQuery[T]
  type AbstractModifyQuery[M] = BaseModifyQuery[M]

  type AbstractFindAndModifyQuery[M, R] = BaseFindAndModifyQuery[M, R]

}
