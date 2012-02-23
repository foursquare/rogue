// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.recordv2


trait Field[V, R] {
  def name: String
  def owner: R
  def defaultValue: V
}
