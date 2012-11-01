// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.field.{Field, OptionalField, RequiredField}
import com.mongodb.DBObject
import java.util.Date
import java.util.regex.Pattern
import org.bson.types.ObjectId
import org.joda.time.DateTime
import scala.util.matching.Regex

object CondOps extends Enumeration(0, "$ne", "$lt", "$gt", "$lte", "$gte",
                                   "$in", "$nin", "$near", "$all", "$size",
                                   "$exists", "$type", "$mod") {
  type Op = Value
  val Ne, Lt, Gt, LtEq, GtEq, In, Nin, Near, All, Size, Exists, Type, Mod = Value
}

object ModOps extends Enumeration(0, "$inc", "$set", "$unset", "$push", "$pushAll",
                                  "$addToSet", "$pop", "$pull", "$pullAll", "$bit",
                                  "$rename") {
  type Op = Value
  val Inc, Set, Unset, Push, PushAll, AddToSet, Pop, Pull, PullAll, Bit, Rename = Value
}

object MongoType extends Enumeration {
  type MongoType = Value
  val Double = Value(1)
  val String = Value(2)
  val Object = Value(3)
  val Array = Value(4)
  val Binary = Value(5)
  val ObjectId = Value(7)
  val Boolean = Value(8)
  val Date = Value(9)
  val Null = Value(10)
  val RegEx = Value(11)
  val JavaScript = Value(13)
  val Symbol = Value(15)
  val Int32 = Value(16)
  val Timestamp = Value(17)
  val Int64 = Value(18)
  val MaxKey = Value(127)
  val MinKey = Value(255)
}

// ********************************************************************************
// *** Query fields
// ********************************************************************************
//
// The types of fields that can be queried, and the particular query operations that each supports
// are defined below.

abstract class AbstractQueryField[F, V, DB, M](val field: Field[F, M]) {
  def valueToDB(v: V): DB
  def valuesToDB(vs: Traversable[V]) = vs.map(valueToDB _)

  def eqs(v: V) = EqClause(field.name, valueToDB(v))
  def neqs(v: V) = new NeQueryClause(field.name, valueToDB(v))
  def in[L <% Traversable[V]](vs: L) = QueryHelpers.inListClause(field.name, valuesToDB(vs))
  def nin[L <% Traversable[V]](vs: L) = new NinQueryClause(field.name, QueryHelpers.validatedList(valuesToDB(vs)))

  def lt(v: V) = new LtQueryClause(field.name, valueToDB(v))
  def gt(v: V) = new GtQueryClause(field.name, valueToDB(v))
  def lte(v: V) = new LtEqQueryClause(field.name, valueToDB(v))
  def gte(v: V) = new GtEqQueryClause(field.name, valueToDB(v))

  def <(v: V) = lt(v)
  def <=(v: V) = lte(v)
  def >(v: V) = gt(v)
  def >=(v: V) = gte(v)

  def between(v1: V, v2: V) =
    new BetweenQueryClause(field.name, valueToDB(v1), valueToDB(v2))

  def between(range: (V, V)) =
    new BetweenQueryClause(field.name, valueToDB(range._1), valueToDB(range._2))

  def exists(b: Boolean) = new ExistsQueryClause(field.name, b)

  def hastype(t: MongoType.Value) = new TypeQueryClause(field.name, t)
}

class QueryField[V, M](field: Field[V, M])
    extends AbstractQueryField[V, V, V, M](field) {
  override def valueToDB(v: V) = v
}

class DateQueryField[M](field: Field[Date, M]) extends AbstractQueryField[Date, DateTime, Date, M](field) {
  override def valueToDB(d: DateTime) = d.toDate

  def before(d: DateTime) = new LtQueryClause(field.name, d.toDate)
  def after(d: DateTime) = new GtQueryClause(field.name, d.toDate)
  def onOrBefore(d: DateTime) = new LtEqQueryClause(field.name, d.toDate)
  def onOrAfter(d: DateTime) = new GtEqQueryClause(field.name, d.toDate)
}

class EnumNameQueryField[M, E <: Enumeration#Value](field: Field[E, M])
    extends AbstractQueryField[E, E, String, M](field) {
  override def valueToDB(e: E) = e.toString
}

class EnumIdQueryField[M, E <: Enumeration#Value](field: Field[E, M])
    extends AbstractQueryField[E, E, Int, M](field) {
  override def valueToDB(e: E) = e.id
}

class GeoQueryField[M](field: Field[LatLong, M])
    extends AbstractQueryField[LatLong, LatLong, java.util.List[Double], M](field) {
  override def valueToDB(ll: LatLong) =
    QueryHelpers.list(List(ll.lat, ll.long))

  def eqs(lat: Double, lng: Double) =
    EqClause(field.name, QueryHelpers.list(List(lat, lng)))

  def neqs(lat: Double, lng: Double) =
    new NeQueryClause(field.name, QueryHelpers.list(List(lat, lng)))

  def near(lat: Double, lng: Double, radius: Degrees) =
    new NearQueryClause(field.name, QueryHelpers.list(List(lat, lng, QueryHelpers.radius(radius))))

  def withinCircle(lat: Double, lng: Double, radius: Degrees) =
    new WithinCircleClause(field.name, lat, lng, QueryHelpers.radius(radius))

  def withinBox(lat1: Double, lng1: Double, lat2: Double, lng2: Double) =
    new WithinBoxClause(field.name, lat1, lng1, lat2, lng2)
}

class NumericQueryField[V, M](field: Field[V, M])
    extends AbstractQueryField[V, V, V, M](field) {
  def mod(by: Int, eq: Int) =
    new ModQueryClause(field.name, QueryHelpers.list(List(by, eq)))
  override def valueToDB(v: V) = v
}


class ObjectIdQueryField[F <: ObjectId, M](override val field: Field[F, M])
    extends NumericQueryField(field) {
  def before(d: DateTime) =
    new LtQueryClause(field.name, new ObjectId(d.toDate, 0, 0))

  def after(d: DateTime) =
    new GtQueryClause(field.name, new ObjectId(d.toDate, 0, 0))

  def between(d1: DateTime, d2: DateTime) =
    new StrictBetweenQueryClause(field.name, new ObjectId(d1.toDate, 0, 0), new ObjectId(d2.toDate, 0, 0))

  def between(range: (DateTime, DateTime)) =
    new StrictBetweenQueryClause(field.name, new ObjectId(range._1.toDate, 0, 0), new ObjectId(range._2.toDate, 0, 0))
}

class ForeignObjectIdQueryField[F <: ObjectId, M, T](
    override val field: Field[F, M],
    val getId: T => F
) extends ObjectIdQueryField[F, M](field) {
  // The implicit parameter is solely to get around the fact that because of
  // erasure, this method and the method in AbstractQueryField look the same.
  def eqs(obj: T)(implicit ev: T =:= T) =
    EqClause(field.name, getId(obj))

  // The implicit parameter is solely to get around the fact that because of
  // erasure, this method and the method in AbstractQueryField look the same.
  def neqs(obj: T)(implicit ev: T =:= T) =
    new NeQueryClause(field.name, getId(obj))

  // The implicit parameter is solely to get around the fact that because of
  // erasure, this method and the method in AbstractQueryField look the same.
  def in(objs: Traversable[T])(implicit ev: T =:= T) =
    QueryHelpers.inListClause(field.name, objs.map(getId))

  // The implicit parameter is solely to get around the fact that because of
  // erasure, this method and the method in AbstractQueryField look the same.
  def nin(objs: Traversable[T])(implicit ev: T =:= T) =
    new NinQueryClause(field.name, QueryHelpers.validatedList(objs.map(getId)))
}

trait StringRegexOps[V, M] { self: AbstractQueryField[V, String, String, M] =>

  def startsWith(s: String): RegexQueryClause[PartialIndexScan] =
    new RegexQueryClause[PartialIndexScan](field.name, PartialIndexScan, Pattern.compile("^" + Pattern.quote(s)))

  def matches(p: Pattern): RegexQueryClause[DocumentScan] =
    new RegexQueryClause[DocumentScan](field.name, DocumentScan, p)

  def matches(r: Regex): RegexQueryClause[DocumentScan] =
    matches(r.pattern)

  def regexWarningNotIndexed(p: Pattern) =
    matches(p)
}

class StringQueryField[M](override val field: Field[String, M])
    extends AbstractQueryField[String, String, String, M](field)
    with StringRegexOps[String, M] {

  override def valueToDB(v: String): String = v
}

class CaseClassQueryField[V, M](val field: Field[V, M]) {
  def unsafeField[F](name: String): SelectableDummyField[F, M] =
    new SelectableDummyField[F, M](field.name + "." + name, field.owner)
}

class BsonRecordQueryField[M, B](field: Field[B, M], asDBObject: B => DBObject, defaultValue: B)
    extends AbstractQueryField[B, B, DBObject, M](field) {
  override def valueToDB(b: B) = asDBObject(b)

  def subfield[V](subfield: B => Field[V, B]): SelectableDummyField[V, M] = {
    new SelectableDummyField[V, M](field.name + "." + subfield(defaultValue).name, field.owner)
  }

  def unsafeField[V](name: String): DummyField[V, M] = {
    new DummyField[V, M](field.name + "." + name, field.owner)
  }

  def subselect[V](f: B => Field[V, B]): SelectableDummyField[V, M] = subfield(f)
}

// This class is a hack to get $pull working for lists of objects. In that case,
// the $pull should look like:
//   "$pull" : { "field" : { "subfield" : { "$gt" : 3 }}}
// whereas for normal queries, the same query would look like:
//   { "field.subfield" : { "$gt" : 3 }}
// So, normally, we need to just have one level of nesting, but here we want two.
class BsonRecordQueryFieldInPullContext[M, B]
    (field: Field[B, M], rec: B, asDBObject: B => DBObject)
    extends AbstractQueryField[B, B, DBObject, M](field) {
  override def valueToDB(b: B) = asDBObject(b)

  def subfield[V](subfield: B => Field[V, B]): SelectableDummyField[V, M] = {
    new SelectableDummyField[V, M](subfield(rec).name, field.owner)
  }
}

abstract class AbstractListQueryField[F, V, DB, M, CC[X] <: Seq[X]](field: Field[CC[F], M]) extends AbstractQueryField[CC[F], V, DB, M](field) {

  def all(vs: Traversable[V]) =
    QueryHelpers.allListClause(field.name, valuesToDB(vs))

  def neqs(vs: Traversable[V]) =
    new NeQueryClause(field.name, QueryHelpers.validatedList(valuesToDB(vs)))

  def size(s: Int) =
    new SizeQueryClause(field.name, s)

  def contains(v: V) =
    EqClause(field.name, valueToDB(v))

  def notcontains(v: V) =
    new NeQueryClause(field.name, valueToDB(v))

  def at(i: Int): DummyField[V, M] =
    new DummyField[V, M](field.name + "." + i.toString, field.owner)

  def idx(i: Int): DummyField[V, M] = at(i)
}

class ListQueryField[V, M](field: Field[List[V], M])
    extends AbstractListQueryField[V, V, V, M, List](field) {
  override def valueToDB(v: V) = v
}

class StringsListQueryField[M](override val field: Field[List[String], M])
    extends AbstractListQueryField[String, String, String, M, List](field)
    with StringRegexOps[List[String], M] {

  override def valueToDB(v: String) = v
}

class SeqQueryField[V, M](field: Field[Seq[V], M])
    extends AbstractListQueryField[V, V, V, M, Seq](field) {
  override def valueToDB(v: V) = v
}

class CaseClassListQueryField[V, M](field: Field[List[V], M])
    extends AbstractListQueryField[V, V, DBObject, M, List](field) {
  override def valueToDB(v: V) = QueryHelpers.asDBObject(v)

  def unsafeField[F](name: String): SelectableDummyField[List[F], M] =
    new SelectableDummyField[List[F], M](field.name + "." + name, field.owner)
}

class BsonRecordListQueryField[M, B](field: Field[List[B], M], rec: B, asDBObject: B => DBObject)
    extends AbstractListQueryField[B, B, DBObject, M, List](field) {
  override def valueToDB(b: B) = asDBObject(b)

  def subfield[V, V1](f: B => Field[V, B])(implicit ev: Rogue.Flattened[V, V1]): SelectableDummyField[List[V1], M] = {
    new SelectableDummyField[List[V1], M](field.name + "." + f(rec).name, field.owner)
  }

  def subselect[V, V1](f: B => Field[V, B])(implicit ev: Rogue.Flattened[V, V1]): SelectField[Option[List[V1]], M] = {
    Rogue.roptionalFieldToSelectField(subfield(f))
  }

  def unsafeField[V](name: String): DummyField[V, M] = {
    new DummyField[V, M](field.name + "." + name, field.owner)
  }
}

class MapQueryField[V, M](val field: Field[Map[String, V], M]) {
  def at(key: String): SelectableDummyField[V, M] =
    new SelectableDummyField(field.name + "." + key, field.owner)
}

class EnumerationListQueryField[V <: Enumeration#Value, M](field: Field[List[V], M])
    extends AbstractListQueryField[V, V, String, M, List](field) {
  override def valueToDB(v: V) = v.toString
}


// ********************************************************************************
// *** Modify fields
// ********************************************************************************


abstract class AbstractModifyField[V, DB, M](val field: Field[V, M]) {
  def valueToDB(v: V): DB
  def setTo(v: V): ModifyClause = new ModifyClause(ModOps.Set, field.name -> valueToDB(v))
  def setTo(vOpt: Option[V]): ModifyClause = vOpt match {
    case Some(v) => setTo(v)
    case none => unset
  }
  def unset = new ModifyClause(ModOps.Unset, field.name -> 1)
  def rename(newName: String) = new ModifyClause(ModOps.Rename, field.name -> newName)
}

class ModifyField[V, M](field: Field[V, M])
    extends AbstractModifyField[V, V, M](field) {
  def valueToDB(v: V) = v
}

class DateModifyField[M](field: Field[Date, M])
    extends AbstractModifyField[Date, Date, M](field) {
  override def valueToDB(d: Date) = d

  def setTo(d: DateTime) = new ModifyClause(ModOps.Set, field.name -> d.toDate)
}

class EnumerationModifyField[M, E <: Enumeration#Value](field: Field[E, M])
    extends AbstractModifyField[E, String, M](field) {
  override def valueToDB(e: E) = e.toString
}

class GeoModifyField[M](field: Field[LatLong, M])
    extends AbstractModifyField[LatLong, java.util.List[Double], M](field) {
  override def valueToDB(ll: LatLong) =
    QueryHelpers.list(List(ll.lat, ll.long))

  def setTo(lat: Double, long: Double) =
    new ModifyClause(ModOps.Set,
                     field.name -> QueryHelpers.list(List(lat, long)))
}

class NumericModifyField[V, M](override val field: Field[V, M]) extends ModifyField[V, M](field) {
  def inc(v: Int) = new ModifyClause(ModOps.Inc, field.name -> v)

  def inc(v: Long) = new ModifyClause(ModOps.Inc, field.name -> v)

  def bitAnd(v: Int) = new ModifyBitAndClause(field.name, v)

  def bitOr(v: Int) = new ModifyBitOrClause(field.name, v)
}

class BsonRecordModifyField[M, B](field: Field[B, M], asDBObject: B => DBObject)
    extends AbstractModifyField[B, DBObject, M](field) {
  override def valueToDB(b: B) = asDBObject(b)
}

class MapModifyField[V, M](field: Field[Map[String, V], M])
    extends AbstractModifyField[Map[String, V], java.util.Map[String, V], M](field) {
  override def valueToDB(m: Map[String, V]) = QueryHelpers.makeJavaMap(m)
}

abstract class AbstractListModifyField[V, DB, M, CC[X] <: Seq[X]](val field: Field[CC[V], M]) {
  def valueToDB(v: V): DB

  def valuesToDB(vs: Traversable[V]) = vs.map(valueToDB _)

  def setTo(vs: Traversable[V]) =
    new ModifyClause(ModOps.Set,
                     field.name -> QueryHelpers.list(valuesToDB(vs)))

  def push(v: V) =
    new ModifyClause(ModOps.Push,
                     field.name -> valueToDB(v))

  def pushAll(vs: Traversable[V]) =
    new ModifyClause(ModOps.PushAll,
                     field.name -> QueryHelpers.list(valuesToDB(vs)))

  def addToSet(v: V) =
    new ModifyClause(ModOps.AddToSet,
                     field.name -> valueToDB(v))

  def addToSet(vs: Traversable[V]) =
    new ModifyAddEachClause(field.name, valuesToDB(vs))

  def popFirst =
    new ModifyClause(ModOps.Pop, field.name -> -1)

  def popLast =
    new ModifyClause(ModOps.Pop, field.name -> 1)

  def pull(v: V) =
    new ModifyClause(ModOps.Pull,
                     field.name -> valueToDB(v))

  def pullAll(vs: Traversable[V]) =
    new ModifyClause(ModOps.PullAll,
                     field.name -> QueryHelpers.list(valuesToDB(vs)))

  def $: Field[V, M] = new DummyField[V, M](field.name + ".$", field.owner)

  def pullWhere(clauseFuncs: (Field[V, M] => QueryClause[_])*) =
    new ModifyPullWithPredicateClause(field.name,
                                      clauseFuncs.map(cf => cf(new DummyField[V, M](field.name, field.owner))): _*)
}

class SeqModifyField[V, M](field: Field[Seq[V], M])
    extends AbstractListModifyField[V, V, M, Seq](field) {
  override def valueToDB(v: V) = v
}

class ListModifyField[V, M](field: Field[List[V], M])
    extends AbstractListModifyField[V, V, M, List](field) {
  override def valueToDB(v: V) = v
}

class CaseClassListModifyField[V, M](field: Field[List[V], M])
    extends AbstractListModifyField[V, DBObject, M, List](field) {
  override def valueToDB(v: V) = QueryHelpers.asDBObject(v)
}

class EnumerationListModifyField[V <: Enumeration#Value, M](field: Field[List[V], M])
    extends AbstractListModifyField[V, String, M, List](field) {
  override def valueToDB(v: V) = v.toString
}

class BsonRecordListModifyField[M, B](field: Field[List[B], M], rec: B, asDBObject: B => DBObject)(implicit mf: Manifest[B])
    extends AbstractListModifyField[B, DBObject, M, List](field) {
  override def valueToDB(b: B) = asDBObject(b)

  // override def $: BsonRecordField[M, B] = {
  //   new BsonRecordField[M, B](field.owner, rec.meta)(mf) {
  //     override def name = field.name + ".$"
  //   }
  // }

  def pullObjectWhere[V](clauseFuncs: BsonRecordQueryFieldInPullContext[M, B] => QueryClause[_]*) = {
    new ModifyPullObjWithPredicateClause(
        field.name,
      clauseFuncs.map(cf => cf(
        new BsonRecordQueryFieldInPullContext(
          new RequiredDummyField[B, M](field.name, field.owner, rec),
          rec,
          asDBObject
        ))): _*)
  }
}

// ********************************************************************************
// *** Select fields
// ********************************************************************************

/**
 * Fields that can be turned into SelectFields can be used in a .select call.
 *
 * This class is sealed because only RequiredFields and OptionalFields should
 * be selectable. Be careful when adding subclasses of this class.
 */
sealed abstract class SelectField[V, M](val field: Field[_, M], val slc: Option[(Int, Option[Int])] = None) {
  // Input will be a Box of the value, and output will either be a Box of the value or the value itself
  def valueOrDefault(v: Option[_]): Any
  def slice(s: Int): SelectField[V, M]
  def slice(s: Int, e: Int): SelectField[V, M]
}

final class MandatorySelectField[V, M](override val field: RequiredField[V, M],
                                       override val slc: Option[(Int, Option[Int])] = None)
    extends SelectField[V, M](field, slc) {
  override def valueOrDefault(v: Option[_]): Any = v.getOrElse(field.defaultValue)
  override def slice(s: Int): MandatorySelectField[V, M] = {
    new MandatorySelectField(field, Some((s, None)))
  }
  override def slice(s: Int, e: Int): MandatorySelectField[V, M] = {
    new MandatorySelectField(field, Some((s, Some(e))))
  }
}

final class OptionalSelectField[V, M](override val field: OptionalField[V, M],
                                      override val slc: Option[(Int, Option[Int])] = None)
    extends SelectField[Option[V], M](field, slc) {
  override def valueOrDefault(v: Option[_]): Any = v
  override def slice(s: Int): OptionalSelectField[V, M] = {
    new OptionalSelectField(field, Some((s, None)))
  }
  override def slice(s: Int, e: Int): OptionalSelectField[V, M] = {
    new OptionalSelectField(field, Some((s, Some(e))))
  }
}

// ********************************************************************************
// *** Dummy field
// ********************************************************************************

class DummyField[V, R](override val name: String, override val owner: R) extends Field[V, R]

class SelectableDummyField[V, R](override val name: String, override val owner: R) extends OptionalField[V, R]

class RequiredDummyField[V, R](override val name: String, override val owner: R, override val defaultValue: V) extends RequiredField[V, R]
