// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.index

case class IndexModifier(value: Any)

object Asc extends IndexModifier(1)
object Desc extends IndexModifier(-1)
object TwoD extends IndexModifier("2d")
