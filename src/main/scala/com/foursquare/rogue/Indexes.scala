// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import net.liftweb.record.Field
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import scala.collection.immutable.ListMap

case class IndexModifier(value: Any)

// Venue.index(_.userid -> Asc)
// Venue.index(_.userid -> Desc)
// Venue.index(_.geolatlng -> TwoD, _.tags -> Asc)
// Venue.index(_.geolatlng -> CrazyCustomIndex)

trait MongoIndex[R <: MongoRecord[R]] {
  def asListMap: ListMap[String, Any]
  protected def t(fld: (Field[_, R], IndexModifier)) = (fld._1.name, fld._2.value)
}

case class MongoIndex1[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier]
                      (f1: (F1, M1)) extends MongoIndex[R] {
  def asListMap = ListMap(t(f1))
}
case class MongoIndex2[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier]
                      (f1: (F1, M1), f2: (F2, M2)) extends MongoIndex[R] {
  def asListMap = ListMap(t(f1), t(f2))
}
case class MongoIndex3[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier]
                      (f1: (F1, M1), f2: (F2, M2), f3: (F3, M3)) extends MongoIndex[R] {
  def asListMap = ListMap(t(f1), t(f2), t(f3))
}
case class MongoIndex4[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier]
                      (f1: (F1, M1), f2: (F2, M2), f3: (F3, M3),
                       f4: (F4, M4)) extends MongoIndex[R] {
  def asListMap = ListMap(t(f1), t(f2), t(f3), t(f4))
}
case class MongoIndex5[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier,
                       F5 <: Field[_, R], M5 <: IndexModifier]
                      (f1: (F1, M1), f2: (F2, M2), f3: (F3, M3),
                       f4: (F4, M4), f5: (F5, M5)) extends MongoIndex[R] {
  def asListMap = ListMap(t(f1), t(f2), t(f3), t(f4), t(f5))
}
case class MongoIndex6[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier,
                       F5 <: Field[_, R], M5 <: IndexModifier,
                       F6 <: Field[_, R], M6 <: IndexModifier]
                      (f1: (F1, M1), f2: (F2, M2), f3: (F3, M3),
                       f4: (F4, M4), f5: (F5, M5), f6: (F6, M6)) extends MongoIndex[R] {
  def asListMap = ListMap(t(f1), t(f2), t(f3), t(f4), t(f5), t(f6))
}

case class IndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]) {
  def index[F1 <: Field[_, M], M1 <: IndexModifier]
           (f1: M => (F1, M1)): MongoIndex1[M, F1, M1] = MongoIndex1[M, F1, M1](f1(rec))
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier]
           (f1: M => (F1, M1), f2: M => (F2, M2)): MongoIndex2[M, F1, M1, F2, M2] =
              MongoIndex2[M, F1, M1, F2, M2](f1(rec), f2(rec))
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier]
           (f1: M => (F1, M1), f2: M => (F2, M2),
            f3: M => (F3, M3)): MongoIndex3[M, F1, M1, F2, M2, F3, M3] =
              MongoIndex3[M, F1, M1, F2, M2, F3, M3](f1(rec), f2(rec), f3(rec))
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier]
           (f1: M => (F1, M1), f2: M => (F2, M2),
            f3: M => (F3, M3), f4: M => (F4, M4)): MongoIndex4[M, F1, M1, F2, M2, F3, M3, F4, M4] =
              MongoIndex4[M, F1, M1, F2, M2, F3, M3, F4, M4](f1(rec), f2(rec), f3(rec), f4(rec))
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier,
            F5 <: Field[_, M], M5 <: IndexModifier]
           (f1: M => (F1, M1), f2: M => (F2, M2),
            f3: M => (F3, M3), f4: M => (F4, M4),
            f5: M => (F5, M5)): MongoIndex5[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5] =
              MongoIndex5[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5](f1(rec), f2(rec), f3(rec), f4(rec), f5(rec))
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier,
            F5 <: Field[_, M], M5 <: IndexModifier,
            F6 <: Field[_, M], M6 <: IndexModifier]
           (f1: M => (F1, M1), f2: M => (F2, M2),
            f3: M => (F3, M3), f4: M => (F4, M4),
            f5: M => (F5, M5), f6: M => (F6, M6)): MongoIndex6[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5, F6, M6] =
              MongoIndex6[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5, F6, M6](f1(rec), f2(rec), f3(rec), f4(rec), f5(rec), f6(rec))
}
