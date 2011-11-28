// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import net.liftweb.record.Field
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import scala.collection.immutable.ListMap

case class IndexModifier(value: Any)

// Venue.index(_.userid, Asc)
// Venue.index(_.userid, Desc)
// Venue.index(_.geolatlng, TwoD)
// Venue.index(_.geolatlng, CrazyCustomIndex)

trait MongoIndex[R <: MongoRecord[R]] {
  def asListMap: ListMap[String, Any]

  override def toString() =
    asListMap.map(fld => "%s:%s".format(fld._1, fld._2)).mkString(", ")
}

case class MongoIndex1[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier]
                      (f1: F1, m1: M1) extends MongoIndex[R] {
  def asListMap = ListMap((f1.name, m1.value))
}

case class MongoIndex2[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier]
                      (f1: F1, m1: M1, f2: F2, m2: M2) extends MongoIndex[R] {
  def asListMap = ListMap((f1.name, m1.value), (f2.name, m2.value))
}

case class MongoIndex3[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier]
                      (f1: F1, m1: M1, f2: F2, m2: M2, f3: F3, m3: M3) extends MongoIndex[R] {
  def asListMap = ListMap((f1.name, m1.value), (f2.name, m2.value), (f3.name, m3.value))
}

case class MongoIndex4[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier]
                      (f1: F1, m1: M1, f2: F2, m2: M2, f3: F3, m3: M3,
                       f4: F4, m4: M4) extends MongoIndex[R] {
  def asListMap = ListMap((f1.name, m1.value), (f2.name, m2.value), (f3.name, m3.value),
    (f4.name, m4.value))
}

case class MongoIndex5[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier,
                       F5 <: Field[_, R], M5 <: IndexModifier]
                      (f1: F1, m1: M1, f2: F2, m2: M2, f3: F3, m3: M3,
                       f4: F4, m4: M4, f5: F5, m5: M5) extends MongoIndex[R] {
  def asListMap = ListMap((f1.name, m1.value), (f2.name, m2.value), (f3.name, m3.value),
    (f4.name, m4.value), (f5.name, m5.value))
}

case class MongoIndex6[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier,
                       F5 <: Field[_, R], M5 <: IndexModifier,
                       F6 <: Field[_, R], M6 <: IndexModifier]
                      (f1: F1, m1: M1, f2: F2, m2: M2, f3: F3, m3: M3,
                       f4: F4, m4: M4, f5: F5, m5: M5, f6: F6, m6: M6) extends MongoIndex[R] {
  def asListMap = ListMap((f1.name, m1.value), (f2.name, m2.value), (f3.name, m3.value),
    (f4.name, m4.value), (f5.name, m5.value), (f6.name, m6.value))
}

case class IndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]) {
  def index[F1 <: Field[_, M], M1 <: IndexModifier]
           (f1: M => F1, m1: M1): MongoIndex1[M, F1, M1] = MongoIndex1[M, F1, M1](f1(rec), m1)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2): MongoIndex2[M, F1, M1, F2, M2] =
              MongoIndex2[M, F1, M1, F2, M2](f1(rec), m1, f2(rec), m2)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3): MongoIndex3[M, F1, M1, F2, M2, F3, M3] =
              MongoIndex3[M, F1, M1, F2, M2, F3, M3](f1(rec), m1, f2(rec), m2, f3(rec), m3)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3, f4: M => F4, m4: M4): MongoIndex4[M, F1, M1, F2, M2, F3, M3, F4, M4] =
              MongoIndex4[M, F1, M1, F2, M2, F3, M3, F4, M4](f1(rec), m1, f2(rec), m2, f3(rec), m3, f4(rec), m4)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier,
            F5 <: Field[_, M], M5 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3, f4: M => F4, m4: M4,
            f5: M => F5, m5: M5): MongoIndex5[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5] =
    MongoIndex5[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5](
      f1(rec), m1, f2(rec), m2, f3(rec), m3, f4(rec), m4, f5(rec), m5)

  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier,
            F5 <: Field[_, M], M5 <: IndexModifier,
            F6 <: Field[_, M], M6 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3, f4: M => F4, m4: M4,
            f5: M => F5, m5: M5, f6: M => F6, m6: M6): MongoIndex6[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5, F6, M6] =
    MongoIndex6[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5, F6, M6](
      f1(rec), m1, f2(rec), m2, f3(rec), m3, f4(rec), m4, f5(rec), m5, f6(rec), m6)
}
