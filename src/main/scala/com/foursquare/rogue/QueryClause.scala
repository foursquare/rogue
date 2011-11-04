// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.BasicDBObjectBuilder
import net.liftweb.record.Field

object IndexBehavior extends Enumeration {
  type IndexBehavior = Value
  val Index = Value("Index")
  val PartialIndexScan = Value("Partial index scan")
  val IndexScan = Value("Index scan")
  val DocumentScan = Value("Document scan")
}

class QueryClause[V](val fieldName: String, conditions: (CondOps.Value, V)*) {
  def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    conditions foreach { case (op, v) => q.add(op.toString, if (signature) 0 else v) }
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

class IndexableQueryClause[V, Ind <: MaybeIndexed](fname: String, conds: (CondOps.Value, V)*)
    extends QueryClause[V](fname, conds: _*)

class AllQueryClause[V](fieldName: String, vs: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], Index](fieldName, CondOps.All -> vs)

class InQueryClause[V](fieldName: String, vs: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], Index](fieldName, CondOps.In -> vs)

class GtQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.Gt -> v)

class GtEqQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.GtEq -> v)

class LtQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.Lt -> v)

class LtEqQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.LtEq -> v)

class BetweenQueryClause[V](fieldName: String, lower: V, upper: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.GtEq -> lower, CondOps.LtEq -> upper)

class StrictBetweenQueryClause[V](fieldName: String, lower: V, upper: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.Gt -> lower, CondOps.Lt -> upper)

class NeQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.Ne -> v)

class NearQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, CondOps.Near -> v)

class ModQueryClause[V](fieldName: String, v: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], IndexScan](fieldName, CondOps.Mod -> v)

class TypeQueryClause(fieldName: String, v: MongoType.Value)
    extends IndexableQueryClause[Int, IndexScan](fieldName, CondOps.Type -> v.id)

class ExistsQueryClause(fieldName: String, v: Boolean)
    extends IndexableQueryClause[Boolean, DocumentScan](fieldName, CondOps.Exists -> v)

class NinQueryClause[V](fieldName: String, vs: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], DocumentScan](fieldName, CondOps.Nin -> vs)

class SizeQueryClause(fieldName: String, v: Int)
    extends IndexableQueryClause[Int, DocumentScan](fieldName, CondOps.Size -> v)

class RawQueryClause(f: BasicDBObjectBuilder => Unit) extends IndexableQueryClause("raw") {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    f(q)
  }
  override lazy val actualIndexBehavior = IndexBehavior.DocumentScan
  override val expectedIndexBehavior = IndexBehavior.DocumentScan
}

class EmptyQueryClause[V](fieldName: String) extends IndexableQueryClause[V, Index](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {}
  override lazy val actualIndexBehavior = IndexBehavior.Index
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new EmptyQueryClause[V](fieldName) { override val expectedIndexBehavior = b }
}

class EqClause[V](fieldName: String, value: V) extends IndexableQueryClause[V, Index](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    q.add(fieldName, if (signature) 0 else value)
  }
  override lazy val actualIndexBehavior = IndexBehavior.Index
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new EqClause(fieldName, value) { override val expectedIndexBehavior = b }
}

class WithinCircleClause[V](fieldName: String, lat: Double, lng: Double, radius: Double) extends IndexableQueryClause[V, PartialIndexScan](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    val value = if (signature) 0 else QueryHelpers.list(List(QueryHelpers.list(List(lat, lng)), radius))
    q.push("$within").add("$center", value).pop
  }
  override lazy val actualIndexBehavior = IndexBehavior.PartialIndexScan
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new WithinCircleClause[V](fieldName, lat, lng, radius) { override val expectedIndexBehavior = b }
}

class WithinBoxClause[V](fieldName: String, lat1: Double, lng1: Double, lat2: Double, lng2: Double) extends IndexableQueryClause[V, PartialIndexScan](fieldName) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    val value = if (signature) 0 else {
      QueryHelpers.list(List(QueryHelpers.list(lat1, lng1), QueryHelpers.list(lat2, lng2)))
    }
    q.push("$within").add("$box", value).pop
  }
  override lazy val actualIndexBehavior = IndexBehavior.PartialIndexScan
  override def withExpectedIndexBehavior(b: IndexBehavior.Value) = new WithinBoxClause[V](fieldName, lat1, lng1, lat2, lng2) { override val expectedIndexBehavior = b }
}

class ModifyClause[V](val operator: ModOps.Value, fields: (String, V)*) {
  def extend(q: BasicDBObjectBuilder): Unit = {
    fields foreach { case (name, value) => q.add(name, value) }
  }
}

class ModifyAddEachClause[V](fieldName: String, values: Traversable[V]) extends ModifyClause[V](ModOps.AddToSet) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("$each", QueryHelpers.list(values)).pop
  }
}

class ModifyBitAndClause[V](fieldName: String, value: V) extends ModifyClause[V](ModOps.Bit) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("and", value).pop
  }
}

class ModifyBitOrClause[V](fieldName: String, value: V) extends ModifyClause[V](ModOps.Bit) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("or", value).pop
  }
}
