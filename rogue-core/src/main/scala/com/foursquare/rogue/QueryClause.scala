// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.DBObject
import com.mongodb.BasicDBObjectBuilder
import java.util.regex.Pattern

abstract class QueryClause[V](val fieldName: String, val actualIndexBehavior: MaybeIndexed, val conditions: (CondOps.Value, V)*) {
  def extend(q: BasicDBObjectBuilder, signature: Boolean) {
    conditions foreach { case (op, v) => q.add(op.toString, if (signature) 0 else v) }
  }
  var negated: Boolean = false
  val expectedIndexBehavior: MaybeIndexed = Index
  def withExpectedIndexBehavior(b: MaybeIndexed): QueryClause[V]
}

abstract class IndexableQueryClause[V, Ind <: MaybeIndexed](fname: String, actualIB: Ind, conds: (CondOps.Value, V)*)
    extends QueryClause[V](fname, actualIB, conds: _*)

trait ShardKeyClause

case class AllQueryClause[V](override val fieldName: String, vs: java.util.List[V], override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[java.util.List[V], Index](fieldName, Index, CondOps.All -> vs) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): AllQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class InQueryClause[V](override val fieldName: String, vs: java.util.List[V], override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[java.util.List[V], Index](fieldName, Index, CondOps.In -> vs) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): InQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class GtQueryClause[V](override val fieldName: String, v: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Gt -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): GtQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class GtEqQueryClause[V](override val fieldName: String, v: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.GtEq -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): GtEqQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class LtQueryClause[V](override val fieldName: String, v: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Lt -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): LtQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class LtEqQueryClause[V](override val fieldName: String, v: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.LtEq -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): LtEqQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class BetweenQueryClause[V](override val fieldName: String, lower: V, upper: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.GtEq -> lower, CondOps.LtEq -> upper) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): BetweenQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class StrictBetweenQueryClause[V](override val fieldName: String, lower: V, upper: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Gt -> lower, CondOps.Lt -> upper) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): StrictBetweenQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class NeQueryClause[V](override val fieldName: String, v: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Ne -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): NeQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class NearQueryClause[V](override val fieldName: String, v: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan, CondOps.Near -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): NearQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class NearSphereQueryClause[V](override val fieldName: String, lat: Double, lng: Double, radians: Radians, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean) {
    q.add(CondOps.NearSphere.toString, if (signature) 0 else QueryHelpers.list(List(lat, lng)))
    q.add(CondOps.MaxDistance.toString, if (signature) 0 else radians.value)
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed): NearSphereQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class ModQueryClause[V](override val fieldName: String, v: java.util.List[V], override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[java.util.List[V], IndexScan](fieldName, IndexScan, CondOps.Mod -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): ModQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class TypeQueryClause(override val fieldName: String, v: MongoType.Value, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[Int, IndexScan](fieldName, IndexScan, CondOps.Type -> v.id) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): TypeQueryClause = this.copy(expectedIndexBehavior = b)
}

case class ExistsQueryClause(override val fieldName: String, v: Boolean, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[Boolean, IndexScan](fieldName, IndexScan, CondOps.Exists -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): ExistsQueryClause = this.copy(expectedIndexBehavior = b)
}

case class NinQueryClause[V](override val fieldName: String, vs: java.util.List[V], override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[java.util.List[V], DocumentScan](fieldName, DocumentScan, CondOps.Nin -> vs) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): NinQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class SizeQueryClause(override val fieldName: String, v: Int, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[Int, DocumentScan](fieldName, DocumentScan, CondOps.Size -> v) {
  override def withExpectedIndexBehavior(b: MaybeIndexed): SizeQueryClause = this.copy(expectedIndexBehavior = b)
}

case class RegexQueryClause[Ind <: MaybeIndexed](override val fieldName: String, actualIB: Ind, p: Pattern, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[Pattern, Ind](fieldName, actualIB) {
  val flagMap = Map(
    Pattern.CANON_EQ -> "c",
    Pattern.CASE_INSENSITIVE -> "i",
    Pattern.COMMENTS -> "x",
    Pattern.DOTALL -> "s",
    Pattern.LITERAL -> "t",
    Pattern.MULTILINE -> "m",
    Pattern.UNICODE_CASE -> "u",
    Pattern.UNIX_LINES -> "d"
  )

  def flagsToString(flags: Int) = {
    (for {
      (mask, char) <- flagMap
      if (flags & mask) != 0
    } yield char).mkString
  }

  override def extend(q: BasicDBObjectBuilder, signature: Boolean) {
    q.add("$regex", if (signature) 0 else p.toString)
    q.add("$options", if (signature) 0 else flagsToString(p.flags))
  }

  override def withExpectedIndexBehavior(b: MaybeIndexed): RegexQueryClause[Ind] = this.copy(expectedIndexBehavior = b)
}


case class RawQueryClause(f: BasicDBObjectBuilder => Unit, override val expectedIndexBehavior: MaybeIndexed = DocumentScan) extends IndexableQueryClause("raw", DocumentScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean) {
    f(q)
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed): RawQueryClause = this.copy(expectedIndexBehavior = b)
}

case class EmptyQueryClause[V](override val fieldName: String, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, Index](fieldName, Index) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean) {}
  override def withExpectedIndexBehavior(b: MaybeIndexed): EmptyQueryClause[V] = this.copy(expectedIndexBehavior = b)
}

case class EqClause[V, Ind <: MaybeIndexed](override val fieldName: String, value: V, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, Index](fieldName, Index) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    q.add(fieldName, if (signature) 0 else value)
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed): EqClause[V, Ind] = this.copy(expectedIndexBehavior = b)
}

case class WithinCircleClause[V](override val fieldName: String, lat: Double, lng: Double, radius: Double, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    val value = if (signature) 0 else QueryHelpers.list(List(QueryHelpers.list(List(lat, lng)), radius))
    q.push("$within").add("$center", value).pop
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed): WithinCircleClause[V] = this.copy(expectedIndexBehavior = b)
}

case class WithinBoxClause[V](override val fieldName: String, lat1: Double, lng1: Double, lat2: Double, lng2: Double, override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, PartialIndexScan](fieldName, PartialIndexScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    val value = if (signature) 0 else {
      QueryHelpers.list(List(QueryHelpers.list(lat1, lng1), QueryHelpers.list(lat2, lng2)))
    }
    q.push("$within").add("$box", value).pop
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed): WithinBoxClause[V] = this.copy(expectedIndexBehavior = b)
}

case class ElemMatchWithPredicateClause[V](override val fieldName: String, clauses: Seq[QueryClause[_]], override val expectedIndexBehavior: MaybeIndexed = Index)
    extends IndexableQueryClause[V, DocumentScan](fieldName, DocumentScan) {
  override def extend(q: BasicDBObjectBuilder, signature: Boolean): Unit = {
    import com.foursquare.rogue.MongoHelpers.AndCondition
    val nested = q.push("$elemMatch")
    MongoHelpers.MongoBuilder.buildCondition(AndCondition(clauses.toList, None), nested, signature)
    nested.pop
  }
  override def withExpectedIndexBehavior(b: MaybeIndexed): ElemMatchWithPredicateClause[V] = this.copy(expectedIndexBehavior = b)
}

class ModifyClause(val operator: ModOps.Value, fields: (String, _)*) {
  def extend(q: BasicDBObjectBuilder): Unit = {
    fields foreach { case (name, value) => q.add(name, value) }
  }
}

class ModifyAddEachClause(fieldName: String, values: Traversable[_])
    extends ModifyClause(ModOps.AddToSet) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("$each", QueryHelpers.list(values)).pop
  }
}

class ModifyPushEachClause(fieldName: String, values: Traversable[_])
    extends ModifyClause(ModOps.Push) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("$each", QueryHelpers.list(values)).pop
  }
}

class ModifyPushEachSliceClause(fieldName: String, slice: Int, values: Traversable[_])
    extends ModifyClause(ModOps.Push) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("$each", QueryHelpers.list(values)).add("$slice", slice).pop
  }
}

class ModifyBitAndClause(fieldName: String, value: Int) extends ModifyClause(ModOps.Bit) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("and", value).pop
  }
}

class ModifyBitOrClause(fieldName: String, value: Int) extends ModifyClause(ModOps.Bit) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    q.push(fieldName).add("or", value).pop
  }
}

class ModifyPullWithPredicateClause[V](fieldName: String, clauses: Seq[QueryClause[_]])
    extends ModifyClause(ModOps.Pull) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    import com.foursquare.rogue.MongoHelpers.AndCondition
    MongoHelpers.MongoBuilder.buildCondition(AndCondition(clauses.toList, None), q, false)
  }
}

class ModifyPullObjWithPredicateClause[V](fieldName: String, clauses: Seq[QueryClause[_]])
    extends ModifyClause(ModOps.Pull) {
  override def extend(q: BasicDBObjectBuilder): Unit = {
    import com.foursquare.rogue.MongoHelpers.AndCondition
    val nested = q.push(fieldName)
    MongoHelpers.MongoBuilder.buildCondition(AndCondition(clauses.toList, None), nested, false)
    nested.pop
  }
}
