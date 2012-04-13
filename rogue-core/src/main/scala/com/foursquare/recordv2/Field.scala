// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.recordv2

trait Field[V, R] {
  def name: String
  def owner: R
}

trait Selectable { self: Field[_, _] => }

trait OptionalField[V, R]  extends Field[V, R] with Selectable

trait RequiredField[V, R] extends Field[V, R] with Selectable {
  def defaultValue: V
}
