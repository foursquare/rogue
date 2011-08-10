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
import net.liftweb.mongodb.record.field.{MongoCaseClassField, MongoCaseClassListField}
import org.bson.types.ObjectId
import org.joda.time._

object CondOps extends Enumeration(0, "$ne", "$lt", "$gt", "$lte", "$gte", "$in", "$nin", "$near", "$all", "$size", "$exists", "$type", "$mod") {
  type Op = Value
  val Ne, Lt, Gt, LtEq, GtEq, In, Nin, Near, All, Size, Exists, Type, Mod = Value
}

object ModOps extends Enumeration(0, "$inc", "$set", "$unset", "$push", "$pushAll", "$addToSet", "$pop", "$pull", "$pullAll") {
  type Op = Value
  val Inc, Set, Unset, Push, PushAll, AddToSet, Pop, Pull, PullAll = Value
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

//////////////////////////////////////////////////////////////////////////////////
/// Query fields
//////////////////////////////////////////////////////////////////////////////////

class QueryField[V, M <: MongoRecord[M]](val field: Field[V, M]) {
  def eqs(v: V) = new EqClause(field.name, v)
  def neqs(v: V) = new QueryClause(field.name, CondOps.Ne -> v)
  def in[L <% Traversable[V]](vs: L) = QueryHelpers.inListClause(field.name, vs)
  def nin[L <% Traversable[V]](vs: L) = new QueryClause(field.name, CondOps.Nin -> QueryHelpers.list(vs))
  def exists(b: Boolean) = new QueryClause(field.name, CondOps.Exists -> b)
  def hastype(t: MongoType.Value) = new QueryClause(field.name, CondOps.Type -> t.id)
}

class CalendarQueryField[M <: MongoRecord[M]](val field: Field[java.util.Calendar, M]) {
  def before(d: DateTime) = new QueryClause(field.name, CondOps.Lt -> d.toDate)
  def after(d: DateTime) = new QueryClause(field.name, CondOps.Gt -> d.toDate)
  def between(d1: DateTime, d2: DateTime) = new QueryClause(field.name, CondOps.Gt -> d1.toDate, CondOps.Lt -> d2.toDate)
}

class EnumerationQueryField[M <: MongoRecord[M], E <: Enumeration#Value](val field: Field[E, M]) {
  def eqs(e: E) = new EqClause(field.name, e.toString)
  def neqs(e: E) = new QueryClause(field.name, CondOps.Ne -> e.toString)
  def in[L <% Traversable[E]](es: L) = QueryHelpers.inListClause(field.name, es.map(_.toString))
  def nin[L <% Traversable[E]](es: L) = new QueryClause(field.name, CondOps.Nin -> QueryHelpers.list(es.map(_.toString)))
}

class GeoQueryField[M <: MongoRecord[M]](val field: Field[LatLong, M]) {
  def eqs(lat: Double, lng: Double) = new EqClause(field.name, QueryHelpers.list(List(lat, lng)))
  def neqs(lat: Double, lng: Double) = new QueryClause(field.name, CondOps.Ne -> QueryHelpers.list(List(lat, lng)))
  def eqs(ll: LatLong) = new EqClause(field.name, QueryHelpers.list(List(ll.lat, ll.long)))
  def neqs(ll: LatLong) = new QueryClause(field.name, CondOps.Ne -> QueryHelpers.list(List(ll.lat, ll.long)))
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
  def between(v1: V, v2: V) = new QueryClause(field.name, CondOps.GtEq -> v1, CondOps.LtEq -> v2)
  def mod(by: Int, eq: Int) = new QueryClause(field.name, CondOps.Mod -> QueryHelpers.list(List(by, eq)))
}

class ObjectIdQueryField[M <: MongoRecord[M]](override val field: Field[ObjectId, M]) extends NumericQueryField(field) {
  def before(d: DateTime) = new QueryClause(field.name, CondOps.Lt -> new ObjectId(d.toDate, 0, 0))
  def after(d: DateTime) = new QueryClause(field.name, CondOps.Gt -> new ObjectId(d.toDate, 0, 0))
  def between(d1: DateTime, d2: DateTime) = new QueryClause(field.name, CondOps.Gt -> new ObjectId(d1.toDate, 0, 0), CondOps.Lt -> new ObjectId(d2.toDate, 0, 0))
}

class StringQueryField[M <: MongoRecord[M]](val field: Field[String, M]) {
  def startsWith(s: String) = new EqClause(field.name, Pattern.compile("^" + Pattern.quote(s)))
  def regexWarningNotIndexed(p: Pattern) = new EqClause(field.name, p)
}

abstract class AbstractListQueryField[V, DB, M <: MongoRecord[M]](val field: Field[List[V], M]) {
  def valueToDB(v: V): DB
  def valuesToDB(vs: Traversable[V]) = vs.map(valueToDB _)

  def all(vs: Traversable[V]) = QueryHelpers.allListClause(field.name, valuesToDB(vs))
  def in(vs: Traversable[V]) = QueryHelpers.inListClause(field.name, valuesToDB(vs))
  def nin(vs: Traversable[V]) = new QueryClause(field.name, CondOps.Nin -> QueryHelpers.list(valuesToDB(vs)))
  def size(s: Int) = new QueryClause(field.name, CondOps.Size -> s)
  def contains(v: V) = new EqClause(field.name, valueToDB(v))
  def notcontains(v: V) = new QueryClause(field.name, CondOps.Ne -> valueToDB(v))
  def at(i: Int): DummyField[V, M] = new DummyField[V, M](field.owner, field.name + "." + i.toString)
  def idx(i: Int): DummyField[V, M] = at(i)
}

class ListQueryField[V, M <: MongoRecord[M]](field: Field[List[V], M]) extends AbstractListQueryField[V, V, M](field) {
  override def valueToDB(v: V) = v
}

class CaseClassQueryField[V, M <: MongoRecord[M]](val field: MongoCaseClassField[M, V]) {
  def unsafeField[F](name: String): SelectableDummyField[F, M] = new SelectableDummyField[F, M](field.owner, field.name + "." + name)
}

class CaseClassListQueryField[V, M <: MongoRecord[M]](field: MongoCaseClassListField[M, V]) extends AbstractListQueryField[V, DBObject, M](field) {
  override def valueToDB(v: V) = QueryHelpers.asDBObject(v)
  def unsafeField[F](name: String): DummyField[F, M] = new DummyField[F, M](field.owner, field.name + "." + name)
}

class MapQueryField[V, M <: MongoRecord[M]](val field: Field[Map[String, V], M]) {
  def at(key: String): SelectableDummyField[V, M] = new SelectableDummyField[V, M](field.owner, field.name + "." + key)
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

abstract class SelectField[V, M <: MongoRecord[M]](val field: Field[_, M]) {
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

trait AbstractDummyField[V, M <: MongoRecord[M]] extends Field[V, M] {
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

class DummyField[V, M <: MongoRecord[M]](override val owner: M, override val name: String) extends AbstractDummyField[V, M]
class SelectableDummyField[V, M <: MongoRecord[M]](override val owner: M, override val name: String) extends OptionalTypedField[V] with AbstractDummyField[V, M]
