// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.DBObject
import net.liftweb.json.{Extraction, Formats, Serializer, TypeInfo}
import net.liftweb.json.JsonAST.{JObject, JValue}
import net.liftweb.mongodb._

sealed case class Degrees(value: Double)
abstract class LatLong(val lat: Double, val long: Double)

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
    def log(msg: => String, timeMillis: Long): Unit
    def warn(msg: => String): Unit
  }

  object NoopQueryLogger extends QueryLogger {
    override def log(msg: => String, timeMillis: Long) {}
    override def warn(msg: => String) {}
  }

  var logger: QueryLogger = NoopQueryLogger

  def makeJavaList[T](sl: Iterable[T]): java.util.List[T] = {
    val list = new java.util.ArrayList[T]()
    for (id <- sl) list.add(id)
    list
  }

  def makeJavaMap[K, V](m: Map[K, V]): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]
    for ((k, v) <- m) map.put(k, v)
    map
  }

  private def getStackTrace(t: Throwable) = {
    import java.io._
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    t.printStackTrace(pw)
    sw.toString
  }

  private def currentStackTrace() = {
    val e = new Exception()
    e.fillInStackTrace()
    getStackTrace(e)
  }

  def list[T](vs: Iterable[T]): java.util.List[T] = {
    val jlist = makeJavaList(vs)
    if (jlist.size() > 2000) {
      throw new Exception("$in query is too big! (%d items)" format (jlist.size()))
    }
    jlist
  }

  def list(vs: Double*): java.util.List[Double] = list(vs)

  def inListClause[V](fieldName: String, vs: Iterable[V]) = {
    if (vs.isEmpty)
      new EmptyQueryClause[java.util.List[V]](fieldName)
    else
      new QueryClause(fieldName, CondOps.In -> QueryHelpers.list(vs))
  }

  def allListClause[V](fieldName: String, vs: Iterable[V]) = {
    if (vs.isEmpty)
      new EmptyQueryClause[java.util.List[V]](fieldName)
    else
      new QueryClause(fieldName, CondOps.All -> QueryHelpers.list(vs))
  }

  private val METERS_PER_LAT_DEGREE: Double = 111111.0
  private val RADIUS_IN_METERS: Int = 6378100

  def degreesFromMeters(geolat: Double, getlong: Double, meters: Int): Degrees = {
    val latDegrees: Double = meters.toDouble / METERS_PER_LAT_DEGREE
    val metersPerLongDegree = (math.Pi / 180) * math.cos(math.toRadians(geolat)) * RADIUS_IN_METERS
    val longDegrees: Double = meters.toDouble / metersPerLongDegree

    Degrees(longDegrees max latDegrees)
  }

  def radius(d: Degrees) = {
    if (d.value > .72) {
      val stack = currentStackTrace()
      logger.warn("radius parameter too large! (%f) going to max at .72 (80km) from:\n" format (d.value, stack))
      .72
    } else d.value
  }

  def asDBObject[T](x: T): DBObject = {
    JObjectParser.parse(Extraction.decompose(x).asInstanceOf[JObject])
  }
}
