// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.DBObject
import java.util.Date
import java.util.regex.Pattern
import org.bson.types.ObjectId
import org.joda.time.DateTime

trait BSONType[T] {
  def asBSONObject(v: T): AnyRef
}

object BSONType {
  def apply[T: BSONType]: BSONType[T] = implicitly[BSONType[T]]

  implicit object BooleanIsBSONType extends BSONType[Boolean] {
    override def asBSONObject(v: Boolean): AnyRef = v: java.lang.Boolean
  }
  implicit object CharIsBSONType extends BSONType[Char] {
    override def asBSONObject(v: Char): AnyRef = v: java.lang.Character
  }
  implicit object ShortIsBSONType extends BSONType[Short] {
    override def asBSONObject(v: Short): AnyRef = v: java.lang.Short
  }
  implicit object IntIsBSONType extends BSONType[Int] {
    override def asBSONObject(v: Int): AnyRef = v: java.lang.Integer
  }
  implicit object LongIsBSONType extends BSONType[Long] {
    override def asBSONObject(v: Long): AnyRef = v: java.lang.Long
  }
  implicit object FloatIsBSONType extends BSONType[Float] {
    override def asBSONObject(v: Float): AnyRef = v: java.lang.Float
  }
  implicit object DoubleIsBSONType extends BSONType[Double] {
    override def asBSONObject(v: Double): AnyRef = v: java.lang.Double
  }
  implicit object DateIsBSONType extends BSONType[Date] {
    override def asBSONObject(v: Date): AnyRef = v
  }
  implicit object DateTimeIsBSONType extends BSONType[DateTime] {
    override def asBSONObject(v: DateTime): AnyRef = v.toDate
  }
  implicit object PatternIsBSONType extends BSONType[Pattern] {
    override def asBSONObject(v: Pattern): AnyRef = v
  }
  implicit object DBObjectIsBSONType extends BSONType[DBObject] {
    override def asBSONObject(v: DBObject): AnyRef = v
  }
  implicit object StringIsBSONType extends BSONType[String] {
    override def asBSONObject(v: String): AnyRef = v
  }
  implicit object ObjectIdISBSONType extends BSONType[ObjectId] {
    override def asBSONObject(v: ObjectId): AnyRef = v
  }

  implicit def ObjectIdSubtypesAreBSONTypes[T <: ObjectId]: BSONType[T] =
    ObjectIdISBSONType.asInstanceOf[BSONType[T]]

  implicit def LongSubtypesAreBSONTypes[T <: java.lang.Long]: BSONType[T] =
    LongIsBSONType.asInstanceOf[BSONType[T]]

  implicit def IntSubtypesAreBSONTypes[T <: java.lang.Integer]: BSONType[T] =
    IntIsBSONType.asInstanceOf[BSONType[T]]

  implicit def StringSubtypesAreBSONTypes[T <: String]: BSONType[T] =
    StringIsBSONType.asInstanceOf[BSONType[T]]

  class ListsOfBSONTypesAreBSONTypes[T: BSONType] extends BSONType[List[T]] {
    override def asBSONObject(v: List[T]): AnyRef = {
      val bsonType = BSONType[T]
      val ret = new java.util.ArrayList[AnyRef](v.size)
      for (x <- v) {
        ret.add(bsonType.asBSONObject(x))
      }
      ret
    }
  }

  implicit def ListsOfBSONTypesAreBSONTypes[T: BSONType]: BSONType[List[T]] = new ListsOfBSONTypesAreBSONTypes[T]

  class SeqsOfBSONTypesAreBSONTypes[T: BSONType] extends BSONType[Seq[T]] {
    override def asBSONObject(v: Seq[T]): AnyRef = {
      val bsonType = BSONType[T]
      val ret = new java.util.ArrayList[AnyRef](v.size)
      for (x <- v) {
        ret.add(bsonType.asBSONObject(x))
      }
      ret
    }
  }

  implicit def SeqsOfBSONTypesAreBSONTypes[T: BSONType]: BSONType[Seq[T]] = new SeqsOfBSONTypesAreBSONTypes[T]
}
