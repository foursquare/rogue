// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.BasicDBObjectBuilder

object IndexBehavior extends Enumeration {
  type IndexBehavior = Value
  val Index = Value("Index")
  val PartialIndexScan = Value("Partial index scan")
  val IndexScan = Value("Index scan")
  val DocumentScan = Value("Document scan")
}

class QueryClause[V](val fieldName: String, conditions: (CondOps.Value, V)*) {
  def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    conditions foreach { case (op, v) => q.add(op.toString, if (signature) 0 else v) }
    q
  }
  lazy val actualIndexBehavior = conditions.head._1 match {
    case CondOps.All | CondOps.In => IndexBehavior.Index
    case CondOps.Gt | CondOps.GtEq | CondOps.Lt | CondOps.LtEq | CondOps.Ne | CondOps.Near => IndexBehavior.PartialIndexScan
    case CondOps.Mod | CondOps.Type  => IndexBehavior.IndexScan
    case CondOps.Exists | CondOps.Nin | CondOps.Size => IndexBehavior.DocumentScan
  }
  val expectedIndexBehavior = IndexBehavior.Index
  def withExpectedIndexBehavior(b: IndexBehavior.Value) = new QueryClause(fieldName, conditions: _*) { override val expectedIndexBehavior = b }
}

class EmptyQueryClause[V](fieldName: String) extends QueryClause[V](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = new BasicDBObjectBuilder
  override lazy val actualIndexBehavior = IndexBehavior.Index
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new EmptyQueryClause[V](fieldName) { override val expectedIndexBehavior = b }
}

class EqClause[V](fieldName: String, value: V) extends QueryClause[V](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    q.add(fieldName, if (signature) 0 else value)
  }
  override lazy val actualIndexBehavior = IndexBehavior.Index
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new EqClause(fieldName, value) { override val expectedIndexBehavior = b }
}

class WithinCircleClause[V](fieldName: String, lat: Double, lng: Double, radius: Double) extends QueryClause(fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    val value = if (signature) 0 else QueryHelpers.list(List(QueryHelpers.list(List(lat, lng)), radius))
    q.push("$within").add("$center", value).pop
  }
  override lazy val actualIndexBehavior = IndexBehavior.PartialIndexScan
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new WithinCircleClause[V](fieldName, lat, lng, radius) { override val expectedIndexBehavior = b }
}

class WithinBoxClause[V](fieldName: String, lat1: Double, lng1: Double, lat2: Double, lng2: Double) extends QueryClause(fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): BasicDBObjectBuilder = {
    val value = if (signature) 0 else {
      QueryHelpers.list(List(QueryHelpers.list(lat1, lng1), QueryHelpers.list(lat2, lng2)))
    }
    q.push("$within").add("$box", value).pop
  }
  override lazy val actualIndexBehavior = IndexBehavior.PartialIndexScan
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new WithinBoxClause[V](fieldName, lat1, lng1, lat2, lng2) { override val expectedIndexBehavior = b }
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
