// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.index

/**
 * A trait that represents the fact that a record type includes a list
 * of the indexes that exist in MongoDB for that type.
 */
trait IndexedRecord[M] {
  val mongoIndexList: List[MongoIndex[_]] = List()
}
