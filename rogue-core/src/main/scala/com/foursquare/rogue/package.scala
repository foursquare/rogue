// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare

package object rogue {
  type AbstractQuery[M, R, +State] = BaseQuery[M, R, State]

  type ModifyQuery[T, State] = BaseModifyQuery[T, State]
  type AbstractModifyQuery[M, State] = BaseModifyQuery[M, State]

  type AbstractFindAndModifyQuery[M, R] = BaseFindAndModifyQuery[M, R]

}
