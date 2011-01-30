// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.DBObject
import java.util.Calendar
import java.util.regex.Pattern
import net.liftweb.common._
import net.liftweb.json.JsonAST.{JInt, JValue}
import net.liftweb.http.js.JE.Num
import net.liftweb.mongodb.record._
import net.liftweb.record._
import net.liftweb.mongodb.record.field.MongoCaseClassListField
import org.bson.types.ObjectId
import org.joda.time._

object CondOps extends Enumeration(0, "$ne", "$lt", "$gt", "$lte", "$gte", "$in", "$nin", "$near", "$all", "$size", "$exists" ) {
  type Op = Value
  val Ne, Lt, Gt, LtEq, GtEq, In, Nin, Near, All, Size, Exists = Value
}

object ModOps extends Enumeration(0, "$inc", "$set", "$unset", "$push", "$pushAll", "$addToSet", "$pop", "$pull", "$pullAll") {
  type Op = Value
  val Inc, Set, Unset, Push, PushAll, AddToSet, Pop, Pull, PullAll = Value
}

//////////////////////////////////////////////////////////////////////////////////
/// Query fields
//////////////////////////////////////////////////////////////////////////////////

class QueryField[V, M <: MongoRecord[M]](val field: Field[V, M]) {
  def eqs(v: V) = new EqClause(field.name, v)
  def neqs(v: V) = new QueryClause(field.name, CondOps.Ne -> v)
  def in[L <% List[V]](vs: L) = QueryHelpers.inListClause(field.name, vs)
  def nin[L <% List[V]](vs: L) = new QueryClause(field.name, CondOps.Nin -> QueryHelpers.list(vs))
  def exists(b: Boolean) = new QueryClause(field.name, CondOps.Exists -> b)
}

class CalendarQueryField[M <: MongoRecord[M]](val field: Field[java.util.Calendar, M]) {
  def before(d: DateTime) = new QueryClause(field.name, CondOps.Lt -> d.toDate)
  def after(d: DateTime) = new QueryClause(field.name, CondOps.Gt -> d.toDate)
  def between(d1: DateTime, d2: DateTime) = new QueryClause(field.name, CondOps.Gt -> d1.toDate, CondOps.Lt -> d2.toDate)
}

class EnumerationQueryField[M <: MongoRecord[M], E <: Enumeration#Value](val field: Field[E, M]) {
  def eqs(e: E) = new EqClause(field.name, e.toString)
  def neqs(e: E) = new QueryClause(field.name, CondOps.Ne -> e.toString)
  def in[L <% Iterable[E]](es: L) = QueryHelpers.inListClause(field.name, es.map(_.toString))
  def nin[L <% Iterable[E]](es: L) = new QueryClause(field.name, CondOps.Nin -> QueryHelpers.list(es.map(_.toString)))
}

class GeoQueryField[M <: MongoRecord[M]](val field: Field[LatLong, M]) {
  def near(lat: Double, lng: Double, radius: Degrees) = new QueryClause(field.name, CondOps.Near -> QueryHelpers.list(List(lat, lng, QueryHelpers.radius(radius))))
  def withinCircle(lat: Double, lng: Double, radius: Degrees) = new WithinCircleClause(field.name, lat, lng, QueryHelpers.radius(radius))
  def withinBox(lat1: Double, lng1: Double, lat2: Double, lng2: Double) = new WithinBoxClause(field.name, lat1, lng1, lat2, lng2)
}

class NumericQueryField[V, M <: MongoRecord[M]](val field: Field[V, M]) {
  def lt(v: V) = new QueryClause(field.name, CondOps.Lt -> v)
  def gt(v: V) = new QueryClause(field.name, CondOps.Gt -> v)
  def lte(v: V) = new QueryClause(field.name, CondOps.LtEq -> v)
  def gte(v: V) = new QueryClause(field.name, CondOps.GtEq -> v)
  def <(v: V) = lt(v)
  def <=(v: V) = lte(v)
  def >(v: V) = gt(v)
  def >=(v: V) = gte(v)
}

class ObjectIdQueryField[M <: MongoRecord[M]](override val field: Field[ObjectId, M]) extends NumericQueryField(field) {
  // TODO(harryh) these are off by 1 second, update when we get a proper ObjectId constructor in the mongo driver
  def before(d: DateTime) = new QueryClause(field.name, CondOps.Lt -> new ObjectId(d.toDate))
  def after(d: DateTime) = new QueryClause(field.name, CondOps.Gt -> new ObjectId(d.toDate))
  def between(d1: DateTime, d2: DateTime) = new QueryClause(field.name, CondOps.Gt -> new ObjectId(d1.toDate), CondOps.Lt -> new ObjectId(d2.toDate))
}

class StringQueryField[M <: MongoRecord[M]](val field: Field[String, M]) {
  def startsWith(s: String) = new EqClause(field.name, Pattern.compile("^" + Pattern.quote(s)))
  def regexWarningNotIndexed(p: Pattern) = new EqClause(field.name, p)
}

abstract class AbstractListQueryField[V, DB, M <: MongoRecord[M]](val field: Field[List[V], M]) {
  def valueToDB(v: V): DB
  def valuesToDB(vs: Iterable[V]) = vs.map(valueToDB _)

  def all(vs: Iterable[V]) = QueryHelpers.allListClause(field.name, valuesToDB(vs))
  def in(vs: Iterable[V]) = QueryHelpers.inListClause(field.name, valuesToDB(vs))
  def size(s: Int) = new QueryClause(field.name, CondOps.Size -> s)
  def contains(v: V) = new EqClause(field.name, valueToDB(v))
  def at(i: Int): DummyField[V, M] = new DummyField[V, M](field.owner, field.name + "." + i.toString)
  def idx(i: Int): DummyField[V, M] = at(i)
}

class ListQueryField[V, M <: MongoRecord[M]](field: Field[List[V], M]) extends AbstractListQueryField[V, V, M](field) {
  override def valueToDB(v: V) = v
}

class CaseClassListQueryField[V, M <: MongoRecord[M]](field: MongoCaseClassListField[M, V]) extends AbstractListQueryField[V, DBObject, M](field) {
  override def valueToDB(v: V) = QueryHelpers.asDBObject(v)
  def unsafeField(name: String): DummyField[AnyVal, M] = new DummyField[AnyVal, M](field.owner, field.name + "." + name)
}

class MapQueryField[V, M <: MongoRecord[M]](val field: Field[Map[String, V], M]) {
  def at(key: String): DummyField[V, M] = new DummyField[V, M](field.owner, field.name + "." + key)
}

class EnumerationListQueryField[V <: Enumeration#Value, M <: MongoRecord[M]](field: Field[List[V], M]) extends AbstractListQueryField[V, String, M](field) {
  override def valueToDB(v: V) = v.toString
}


//////////////////////////////////////////////////////////////////////////////////
/// Modify fields
//////////////////////////////////////////////////////////////////////////////////

class ModifyField[V, M <: MongoRecord[M]](val field: Field[V, M]) {
  def valueToDB(v: Any) = v match {
    case c: Calendar => c.getTime
    case e: Enumeration#Value => e.toString
    case ll: LatLong => QueryHelpers.list(List(ll.lat, ll.long))
    case m: Map[_, _] => QueryHelpers.makeJavaMap(m)
    case v => v
  }

  def setTo(v: V) = new ModifyClause(ModOps.Set, field.name -> valueToDB(v))
  def unset = new ModifyClause(ModOps.Unset, field.name -> 1)
}

class CalendarModifyField[M <: MongoRecord[M]](val field: Field[Calendar, M]) {
  def setTo(c: Calendar) = new ModifyClause(ModOps.Set, field.name -> c.getTime)
  def setTo(d: DateTime) = new ModifyClause(ModOps.Set, field.name -> d.toDate)
}

class GeoModifyField[M <: MongoRecord[M]](val field: Field[LatLong, M]) {
  def setTo(ll: LatLong) = new ModifyClause(ModOps.Set, field.name -> QueryHelpers.list(List(ll.lat, ll.long)))
  def setTo(lat: Double, long: Double) = new ModifyClause(ModOps.Set, field.name -> QueryHelpers.list(List(lat, long)))
}

class NumericModifyField[V, M <: MongoRecord[M]](val field: Field[V, M]) {
  def inc(v: V) = new ModifyClause(ModOps.Inc, field.name -> v)
}

abstract class AbstractListModifyField[V, DB, M <: MongoRecord[M]](val field: Field[List[V], M]) {
  def valueToDB(v: V): DB
  def valuesToDB(vs: List[V]) = vs.map(valueToDB _)

  def setTo(vs: List[V]) = new ModifyClause(ModOps.Set, field.name -> QueryHelpers.list(valuesToDB(vs)))
  def push(v: V) = new ModifyClause(ModOps.Push, field.name -> valueToDB(v))
  def pushAll(vs: List[V]) = new ModifyClause(ModOps.PushAll, field.name -> QueryHelpers.list(valuesToDB(vs)))
  def addToSet(v: V) = new ModifyClause(ModOps.AddToSet, field.name -> valueToDB(v))
  def addToSet(vs: List[V]) = new ModifyAddEachClause(field.name, valuesToDB(vs))
  def popFirst = new ModifyClause(ModOps.Pop, field.name -> -1)
  def popLast = new ModifyClause(ModOps.Pop, field.name -> 1)
  def pull(v: V) = new ModifyClause(ModOps.Pull, field.name -> valueToDB(v))
  def pullAll(vs: List[V]) = new ModifyClause(ModOps.PullAll, field.name -> QueryHelpers.list(valuesToDB(vs)))
}

class ListModifyField[V, M <: MongoRecord[M]](field: Field[List[V], M]) extends AbstractListModifyField[V, V, M](field) {
  override def valueToDB(v: V) = v
}

class CaseClassListModifyField[V, M <: MongoRecord[M]](field: MongoCaseClassListField[M, V]) extends AbstractListModifyField[V, DBObject, M](field) {
  override def valueToDB(v: V) = QueryHelpers.asDBObject(v)
}

class EnumerationListModifyField[V <: Enumeration#Value, M <: MongoRecord[M]](field: Field[List[V], M]) extends AbstractListModifyField[V, String, M](field) {
  override def valueToDB(v: V) = v.toString
}


//////////////////////////////////////////////////////////////////////////////////
/// Select fields
//////////////////////////////////////////////////////////////////////////////////

abstract class SelectField[V, M <: MongoRecord[M]](val field: Field[_, _]) {
  // Input will be a Box of the value, and output will either be a Box of the value or the value itself
  def apply(v: Any): Any
}

class MandatorySelectField[V, M <: MongoRecord[M]](override val field: Field[V, M] with MandatoryTypedField[V]) extends SelectField[V, M](field) {
  override def apply(v: Any): Any = v.asInstanceOf[Box[V]].openOr(field.defaultValue)
}

class OptionalSelectField[V, M <: MongoRecord[M]](override val field: Field[V, M] with OptionalTypedField[V]) extends SelectField[Box[V], M](field) {
  override def apply(v: Any): Any = v.asInstanceOf[Box[V]]
}


//////////////////////////////////////////////////////////////////////////////////
/// Dummy field
//////////////////////////////////////////////////////////////////////////////////

class DummyField[V, M <: MongoRecord[M]](override val owner: M, override val name: String) extends Field[V, M] {
  override val asJValue = JInt(0)
  override val asJs = Num(0)
  override val toForm = Empty
  override def toBoxMyType(v: ValueType) = Empty
  override def toValueType(v: Box[MyType]) = null.asInstanceOf[ValueType]
  override def defaultValueBox = Empty
  override def set(v: ValueType) = v
  override def get = null.asInstanceOf[ValueType]
  override def is = get
  override def apply(v: V) = owner
  override def setFromAny(a: Any) = Empty
  override def setFromString(s: String) = Empty
  override def setFromJValue(jv: JValue) = Empty
  override def liftSetFilterToBox(in: Box[MyType]): Box[MyType] = Empty
}