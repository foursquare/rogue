// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.Rogue.GenericQuery
import com.mongodb.DBObject
import net.liftweb.json.{Extraction, Formats, Serializer, TypeInfo}
import net.liftweb.json.JsonAST.{JObject, JValue}
import net.liftweb.mongodb._
import net.liftweb.mongodb.record._

case class Degrees(value: Double)
case class LatLong(lat: Double, long: Double)

trait HasMongoForeignObjectId[RefType <: MongoRecord[RefType] with MongoId[RefType]]

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
    def log(query: GenericQuery[_, _], msg: => String, timeMillis: Long): Unit
    def logIndexMismatch(query: GenericQuery[_, _], msg: => String)
    def logIndexHit(query: GenericQuery[_, _], index: MongoIndex[_])
    def warn(query: GenericQuery[_, _], msg: => String): Unit
  }

  class DefaultQueryLogger extends QueryLogger {
    override def log(query: GenericQuery[_, _], msg: => String, timeMillis: Long) {}
    override def logIndexMismatch(query: GenericQuery[_, _], msg: => String) {}
    override def logIndexHit(query: GenericQuery[_, _], index: MongoIndex[_]) {}
    override def warn(query: GenericQuery[_, _], msg: => String) {}
  }

  object NoopQueryLogger extends DefaultQueryLogger

  var logger: QueryLogger = NoopQueryLogger

  trait QueryValidator {
    def validateList[T](xs: Traversable[T]): Unit
    def validateRadius(d: Degrees): Degrees
    def validateQuery[M <: MongoRecord[M]](query: BaseQuery[M, _, _, _, _, _, _]): Unit
    def validateModify[M <: MongoRecord[M]](modify: BaseModifyQuery[M]): Unit
    def validateFindAndModify[M <: MongoRecord[M], R](modify: BaseFindAndModifyQuery[M, R]): Unit
  }

  class DefaultQueryValidator extends QueryValidator {
    override def validateList[T](xs: Traversable[T]) {}
    override def validateRadius(d: Degrees) = d
    override def validateQuery[M <: MongoRecord[M]](query: BaseQuery[M, _, _, _, _, _, _]) {}
    override def validateModify[M <: MongoRecord[M]](modify: BaseModifyQuery[M]) {}
    override def validateFindAndModify[M <: MongoRecord[M], R](modify: BaseFindAndModifyQuery[M, R]) {}
  }

  object NoopQueryValidator extends DefaultQueryValidator

  var validator: QueryValidator = NoopQueryValidator

  trait QueryTransformer {
    def transformQuery[M <: MongoRecord[M]](query: BaseQuery[M, _, _, _, _, _, _]): BaseQuery[M, _, _, _, _, _, _]
    def transformModify[M <: MongoRecord[M]](modify: BaseModifyQuery[M]): BaseModifyQuery[M]
    def transformFindAndModify[M <: MongoRecord[M], R](modify: BaseFindAndModifyQuery[M, R]): BaseFindAndModifyQuery[M, R]
  }

  class DefaultQueryTransformer extends QueryTransformer {
    override def transformQuery[M <: MongoRecord[M]](query: BaseQuery[M, _, _, _, _, _, _]): BaseQuery[M, _, _, _, _, _, _] = { query }
    override def transformModify[M <: MongoRecord[M]](modify: BaseModifyQuery[M]): BaseModifyQuery[M] = { modify }
    override def transformFindAndModify[M <: MongoRecord[M], R](modify: BaseFindAndModifyQuery[M, R]): BaseFindAndModifyQuery[M, R] = { modify }
  }

  object NoopQueryTransformer extends DefaultQueryTransformer

  var transformer: QueryTransformer = NoopQueryTransformer

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

  def orConditionFromQueries(subqueries: List[AbstractQuery[_, _, _, _, _, _, _]]) = {
    MongoHelpers.OrCondition(subqueries.flatMap(subquery => {
      subquery match {
        case q: BaseQuery[_, _, _, _, _, _, _] => Some(q.condition)
        case _ => None
      }
    }))
  }

}
