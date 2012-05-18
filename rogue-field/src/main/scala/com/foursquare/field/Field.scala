// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.field

trait Field[V, R] {
  def name: String
  def owner: R
}

trait OptionalField[V, R]  extends Field[V, R]

trait RequiredField[V, R] extends Field[V, R] {
  def defaultValue: V
}
