// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import net.liftweb.record.Field
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}

case class IndexModifier(value: Any)

// Venue.index(_.userid, Asc)
// Venue.index(_.userid, Desc)
// Venue.index(_.geolatlng, TwoD)
// Venue.index(_.geolatlng, CrazyCustomIndex)

case class MongoIndex1[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier]
                      (f1: R => F1, m1: M1)
case class MongoIndex2[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier]
                      (f1: R => F1, m1: M1, f2: R => F2, m2: M2)
case class MongoIndex3[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier]
                      (f1: R => F1, m1: M1, f2: R => F2, m2: M2, f3: R => F3, m3: M3)
case class MongoIndex4[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier]
                      (f1: R => F1, m1: M1, f2: R => F2, m2: M2, f3: R => F3, M3: M3,
                       f4: R => F4, m4: M4)
case class MongoIndex5[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier,
                       F5 <: Field[_, R], M5 <: IndexModifier]
                      (f1: R => F1, m1: M1, f2: R => F2, m2: M2, f3: R => F3, m3: M3,
                       f4: R => F4, m4: M4, f5: R => F5, m5: M5)
case class MongoIndex6[R <: MongoRecord[R],
                       F1 <: Field[_, R], M1 <: IndexModifier,
                       F2 <: Field[_, R], M2 <: IndexModifier,
                       F3 <: Field[_, R], M3 <: IndexModifier,
                       F4 <: Field[_, R], M4 <: IndexModifier,
                       F5 <: Field[_, R], M5 <: IndexModifier,
                       F6 <: Field[_, R], M6 <: IndexModifier]
                      (f1: R => F1, m1: M1, f2: R => F2, m2: M2, f3: R => F3, m3: M3,
                       f4: R => F4, m4: M4, f5: R => F5, m5: M5, f6: R => F6, m6: M6)

case class IndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]) {
  def index[F1 <: Field[_, M], M1 <: IndexModifier]
           (f1: M => F1, m1: M1): MongoIndex1[M, F1, M1] = MongoIndex1[M, F1, M1](f1, m1)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2): MongoIndex2[M, F1, M1, F2, M2] =
              MongoIndex2[M, F1, M1, F2, M2](f1, m1, f2, m2)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3): MongoIndex3[M, F1, M1, F2, M2, F3, M3] =
              MongoIndex3[M, F1, M1, F2, M2, F3, M3](f1, m1, f2, m2, f3, m3)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3, f4: M => F4, m4: M4): MongoIndex4[M, F1, M1, F2, M2, F3, M3, F4, M4] =
              MongoIndex4[M, F1, M1, F2, M2, F3, M3, F4, M4](f1, m1, f2, m2, f3, m3, f4, m4)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier,
            F5 <: Field[_, M], M5 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3, f4: M => F4, m4: M4,
            f5: M => F5, m5: M5): MongoIndex5[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5] =
              MongoIndex5[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5](f1, m1, f2, m2, f3, m3, f4, m4, f5, m5)
  def index[F1 <: Field[_, M], M1 <: IndexModifier,
            F2 <: Field[_, M], M2 <: IndexModifier,
            F3 <: Field[_, M], M3 <: IndexModifier,
            F4 <: Field[_, M], M4 <: IndexModifier,
            F5 <: Field[_, M], M5 <: IndexModifier,
            F6 <: Field[_, M], M6 <: IndexModifier]
           (f1: M => F1, m1: M1, f2: M => F2, m2: M2,
            f3: M => F3, m3: M3, f4: M => F4, m4: M4,
            f5: M => F5, m5: M5, f6: M => F6, m6: M6): MongoIndex6[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5, F6, M6] =
              MongoIndex6[M, F1, M1, F2, M2, F3, M3, F4, M4, F5, M5, F6, M6](f1, m1, f2, m2, f3, m3, f4, m4, f5, m5, f6, m6)
}
