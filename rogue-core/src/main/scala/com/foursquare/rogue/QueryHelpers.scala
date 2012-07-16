// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.index.MongoIndex
import com.mongodb.{DBObject, WriteConcern}
import net.liftweb.json.{Extraction, Formats, Serializer, TypeInfo}
import net.liftweb.json.JsonAST.{JObject, JValue}
import net.liftweb.mongodb.{JObjectParser, ObjectIdSerializer}

case class Degrees(value: Double)
case class LatLong(lat: Double, long: Double)

object QueryHelpers {
  class DBObjectSerializer extends Serializer[DBObject] {
    val DBObjectClass = classOf[DBObject]

    def deserialize(implicit formats: Formats): PartialFunction[(TypeInfo, JValue), DBObject] = {
      case (TypeInfo(klass, _), json : JObject) if DBObjectClass.isAssignableFrom(klass) =>
        JObjectParser.parse(json)
    }

    def serialize(implicit formats: Formats): PartialFunction[Any, JValue] = {
      case x: DBObject =>
        JObjectParser.serialize(x)
    }
  }

  private implicit val formats =
    (net.liftweb.json.DefaultFormats + new ObjectIdSerializer + new DBObjectSerializer)

  trait QueryLogger {
    def log(query: Query[_, _, _], msg: => String, timeMillis: Long): Unit
    def callback[T](query: Query[_, _, _], msg: => String, func: => T): T
    def logIndexMismatch(query: Query[_, _, _], msg: => String)
    def logIndexHit(query: Query[_, _, _], index: MongoIndex[_])
    def warn(query: Query[_, _, _], msg: => String): Unit
  }

  class DefaultQueryLogger extends QueryLogger {
    override def log(query: Query[_, _, _], msg: => String, timeMillis: Long) {}
    override def callback[T](query: Query[_, _, _], msg: => String, func: => T): T = func
    override def logIndexMismatch(query: Query[_, _, _], msg: => String) {}
    override def logIndexHit(query: Query[_, _, _], index: MongoIndex[_]) {}
    override def warn(query: Query[_, _, _], msg: => String) {}
  }

  object NoopQueryLogger extends DefaultQueryLogger

  var logger: QueryLogger = NoopQueryLogger

  trait QueryValidator {
    def validateList[T](xs: Traversable[T]): Unit
    def validateRadius(d: Degrees): Degrees
    def validateQuery[M](query: Query[M, _, _]): Unit
    def validateModify[M](modify: ModifyQuery[M, _]): Unit
    def validateFindAndModify[M, R](modify: FindAndModifyQuery[M, R]): Unit
  }

  class DefaultQueryValidator extends QueryValidator {
    override def validateList[T](xs: Traversable[T]) {}
    override def validateRadius(d: Degrees) = d
    override def validateQuery[M](query: Query[M, _, _]) {}
    override def validateModify[M](modify: ModifyQuery[M, _]) {}
    override def validateFindAndModify[M, R](modify: FindAndModifyQuery[M, R]) {}
  }

  object NoopQueryValidator extends DefaultQueryValidator

  var validator: QueryValidator = NoopQueryValidator

  trait QueryTransformer {
    def transformQuery[M](query: Query[M, _, _]): Query[M, _, _]
    def transformModify[M](modify: ModifyQuery[M, _]): ModifyQuery[M, _]
    def transformFindAndModify[M, R](modify: FindAndModifyQuery[M, R]): FindAndModifyQuery[M, R]
  }

  class DefaultQueryTransformer extends QueryTransformer {
    override def transformQuery[M](query: Query[M, _, _]): Query[M, _, _] = { query }
    override def transformModify[M](modify: ModifyQuery[M, _]): ModifyQuery[M, _] = { modify }
    override def transformFindAndModify[M, R](modify: FindAndModifyQuery[M, R]): FindAndModifyQuery[M, R] = { modify }
  }

  object NoopQueryTransformer extends DefaultQueryTransformer

  var transformer: QueryTransformer = NoopQueryTransformer

  trait QueryConfig {
    def defaultWriteConcern: WriteConcern
  }

  class DefaultQueryConfig extends QueryConfig {
    override def defaultWriteConcern = WriteConcern.NONE
  }

  object DefaultQueryConfig extends DefaultQueryConfig

  var config: QueryConfig = DefaultQueryConfig

  def makeJavaList[T](sl: Traversable[T]): java.util.List[T] = {
    val list = new java.util.ArrayList[T]()
    for (id <- sl) list.add(id)
    list
  }

  def validatedList[T](vs: Traversable[T]): java.util.List[T] = {
    validator.validateList(vs)
    makeJavaList(vs)
  }

  def list[T](vs: Traversable[T]): java.util.List[T] = {
    makeJavaList(vs)
  }

  def list(vs: Double*): java.util.List[Double] = list(vs)

  def radius(d: Degrees) = {
    validator.validateRadius(d).value
  }

  def makeJavaMap[K, V](m: Map[K, V]): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]
    for ((k, v) <- m) map.put(k, v)
    map
  }

  def inListClause[V](fieldName: String, vs: Traversable[V]) = {
    if (vs.isEmpty)
      new EmptyQueryClause[java.util.List[V]](fieldName)
    else
      new InQueryClause(fieldName, QueryHelpers.validatedList(vs.toSet))
  }

  def allListClause[V](fieldName: String, vs: Traversable[V]) = {
    if (vs.isEmpty)
      new EmptyQueryClause[java.util.List[V]](fieldName)
    else
      new AllQueryClause(fieldName, QueryHelpers.validatedList(vs.toSet))
  }

  def asDBObject[T](x: T): DBObject = {
    JObjectParser.parse(Extraction.decompose(x).asInstanceOf[JObject])
  }

  def orConditionFromQueries(subqueries: List[Query[_, _, _]]) = {
    MongoHelpers.OrCondition(subqueries.flatMap(subquery => {
      subquery match {
        case q: Query[_, _, _] => Some(q.condition)
        case _ => None
      }
    }))
  }
}
