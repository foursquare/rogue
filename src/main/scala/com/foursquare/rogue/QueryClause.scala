// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.BasicDBObjectBuilder

class QueryClause[V](val fieldName: String, conditions: (CondOps.Value, V)*) {
  def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    conditions foreach { case (op, v) => q.add(op.toString, if (signature) 0 else v) }
    q
  }
}

class EmptyQueryClause[V](fieldName: String) extends QueryClause[V](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = new BasicDBObjectBuilder
}

class EqClause[V](fieldName: String, value: V) extends QueryClause[V](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    q.add(fieldName, if (signature) 0 else value)
  }
}

class WithinCircleClause[V](fieldName: String, lat: Double, lng: Double, radius: Double) extends QueryClause(fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    val value = if (signature) 0 else QueryHelpers.list(List(QueryHelpers.list(List(lat, lng)), radius))
    q.push("$within").add("$center", value).pop
  }
}

class WithinBoxClause[V](fieldName: String, lat1: Double, lng1: Double, lat2: Double, lng2: Double) extends QueryClause(fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    val value = if (signature) 0 else {
      QueryHelpers.list(List(QueryHelpers.list(lat1, lng1), QueryHelpers.list(lat2, lng2)))
    }
    q.push("$within").add("$box", value).pop
  }
}

class ModifyClause[V](val operator: ModOps.Value, fields: (String, V)*) {
  def extend(q: BasicDBObjectBuilder): BasicDBObjectBuilder = {
    fields foreach { case (name, value) => q.add(name, value) }
    q
  }
}

class ModifyAddEachClause[V](fieldName: String, values: List[V]) extends ModifyClause[V](ModOps.AddToSet) {
  override def extend(q: BasicDBObjectBuilder): BasicDBObjectBuilder = {
    q.push(fieldName).add("$each", QueryHelpers.list(values)).pop
  }
}
