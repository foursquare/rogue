// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.DBObject
import com.mongodb.BasicDBObjectBuilder
import net.liftweb.record.Field

class QueryClause[V](val fieldName: String, val actualIndexBehavior: MaybeIndexed, conditions: (CondOps.Value, V)*) {
  def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    conditions foreach { case (op, v) => q.add(op.toString, if (signature) 0 else v) }
  }
  val expectedIndexBehavior: MaybeIndexed = Index
  def withExpectedIndexBehavior(b: MaybeIndexed) = new QueryClause(fieldName, actualIndexBehavior, conditions: _*) {
    override val expectedIndexBehavior = b
  }
}

class IndexableQueryClause[V, Ind <: MaybeIndexed](fname: String, actualIB: Ind, conds: (CondOps.Value, V)*)
    extends QueryClause[V](fname, actualIB, conds: _*)

class AllQueryClause[V](fieldName: String, vs: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], Index](fieldName, Index, CondOps.All -> vs)

class InQueryClause[V](fieldName: String, vs: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], Index](fieldName, Index, CondOps.In -> vs)

class GtQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Gt -> v)

class GtEqQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.GtEq -> v)

class LtQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Lt -> v)

class LtEqQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.LtEq -> v)

class BetweenQueryClause[V](fieldName: String, lower: V, upper: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.GtEq -> lower, CondOps.LtEq -> upper)

class StrictBetweenQueryClause[V](fieldName: String, lower: V, upper: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Gt -> lower, CondOps.Lt -> upper)

class NeQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Ne -> v)

class NearQueryClause[V](fieldName: String, v: V)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Near -> v)

class ModQueryClause[V](fieldName: String, v: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], IndexScan](fieldName, IndexScan, CondOps.Mod -> v)

class TypeQueryClause(fieldName: String, v: MongoType.Value)
    extends IndexableQueryClause[Int, IndexScan](fieldName, IndexScan, CondOps.Type -> v.id)

class ExistsQueryClause(fieldName: String, v: Boolean)
    extends IndexableQueryClause[Boolean, DocumentScan](fieldName, DocumentScan, CondOps.Exists -> v)

class NinQueryClause[V](fieldName: String, vs: java.util.List[V])
    extends IndexableQueryClause[java.util.List[V], DocumentScan](fieldName, DocumentScan, CondOps.Nin -> vs)

class SizeQueryClause(fieldName: String, v: Int)
    extends IndexableQueryClause[Int, DocumentScan](fieldName, DocumentScan, CondOps.Size -> v)

class RawQueryClause(f: BasicDBObjectBuilder => Unit) extends IndexableQueryClause("raw", DocumentScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    f(q)
  }
  override val expectedIndexBehavior = DocumentScan
}

class EmptyQueryClause[V](fieldName: String) extends IndexableQueryClause[V, Index](fieldName, Index) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {}
  override def withExpectedIndexBehavior(b: MaybeIndexed) = {
    new EmptyQueryClause[V](fieldName) {
      override val expectedIndexBehavior = b
    }
  }
}

class EqClause[V, Ind <: MaybeIndexed](fieldName: String, actualIB: Ind, value: V) extends IndexableQueryClause[V, Ind](fieldName, actualIB) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    q.add(fieldName, if (signature) 0 else value)
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed) = new EqClause[V, Ind](fieldName, actualIB, value) {
    override val expectedIndexBehavior = b
  }
}

object EqClause {
  def apply[V](fieldName: String, value: V) = {
    new EqClause[V, Index](fieldName, Index, value)
  }
}

class WithinCircleClause[V](fieldName: String, lat: Double, lng: Double, radius: Double) extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    val value = if (signature) 0 else QueryHelpers.list(List(QueryHelpers.list(List(lat, lng)), radius))
    q.push("$within").add("$center", value).pop
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed) = {
    new WithinCircleClause[V](fieldName, lat, lng, radius) {
      override val expectedIndexBehavior = b
    }
  }
}

class WithinBoxClause[V](fieldName: String, lat1: Double, lng1: Double, lat2: Double, lng2: Double) extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    val value = if (signature) 0 else {
      QueryHelpers.list(List(QueryHelpers.list(lat1, lng1), QueryHelpers.list(lat2, lng2)))
    }
    q.push("$within").add("$box", value).pop
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed) = new WithinBoxClause[V](fieldName, lat1, lng1, lat2, lng2) {
    override val expectedIndexBehavior = b
  }
}

class ModifyClause[V](val operator: ModOps.Value, fields: (String, V)*) {
  def extend(q: BasicDBObjectBuilder): Unit = {
    fields foreach { case (name, value) => q.add(name, value) }
  }
}

class ModifyAddEachClause[V](fieldName: String, values: Traversable[V])
    extends ModifyClause[V](ModOps.AddToSet) {
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

class ModifyPullWithPredicateClause[V](fieldName: String, clauses: QueryClause[_]*)
    extends ModifyClause[DBObject](ModOps.Pull) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    import com.foursquare.rogue.MongoHelpers.AndCondition
    MongoHelpers.MongoBuilder.buildCondition(AndCondition(clauses.toList, None), q, false)
  }
}

class ModifyPullObjWithPredicateClause[V](fieldName: String, clauses: QueryClause[_]*)
    extends ModifyClause[DBObject](ModOps.Pull) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    import com.foursquare.rogue.MongoHelpers.AndCondition
    val nested = q.push(fieldName)
    MongoHelpers.MongoBuilder.buildCondition(AndCondition(clauses.toList, None), nested, false)
    nested.pop
  }
}
