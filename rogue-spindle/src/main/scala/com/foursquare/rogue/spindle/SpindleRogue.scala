// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.field.Field
import com.foursquare.rogue.Rogue
import com.foursquare.spindle.{Enum, Record, MetaRecord}

trait SpindleRogue extends Rogue {
  implicit def enumFieldToSpindleEnumQueryField[M <: MetaRecord[_], F <: Enum[F]](f: Field[F, M]): SpindleEnumQueryField[M, F] =
    new SpindleEnumQueryField(f)
  implicit def enumListFieldToSpindleEnumListQueryField[M <: MetaRecord[_], F <: Enum[F]](f: Field[Seq[F], M]): SpindleEnumListQueryField[M, F] =
    new SpindleEnumListQueryField(f)
  implicit def enumFieldToSpindleEnumModifyField[M <: MetaRecord[_], F <: Enum[F]](f: Field[F, M]): SpindleEnumModifyField[M, F] =
    new SpindleEnumModifyField(f)  
  implicit def enumFieldToSpindleEnumListModifyField[M <: MetaRecord[_], F <: Enum[F]](f: Field[Seq[F], M]): SpindleEnumListModifyField[M, F] =
    new SpindleEnumListModifyField(f)  

  implicit def embeddedFieldToSpindleEmbeddedRecordQueryField[
      R <: Record[_],
      MM <: MetaRecord[_]
  ](
      f: Field[R, MM]
  ): SpindleEmbeddedRecordQueryField[R, MM] = new SpindleEmbeddedRecordQueryField(f)

  implicit def embeddedFieldToSpindleEmbeddedRecordModifyField[
      R <: Record[_],
      MM <: MetaRecord[_]
  ](
      f: Field[R, MM]
  ): SpindleEmbeddedRecordModifyField[R, MM] = new SpindleEmbeddedRecordModifyField(f)

  implicit def embeddedListFieldToSpindleEmbeddedRecordListQueryField[
      R <: Record[_],
      MM <: MetaRecord[_]
  ](
      f: Field[Seq[R], MM]
  ): SpindleEmbeddedRecordListQueryField[R, MM] = new SpindleEmbeddedRecordListQueryField(f)

  implicit def embeddedListFieldToSpindleEmbeddedRecordListModifyField[
      R <: Record[_],
      MM <: MetaRecord[_]
  ](
      f: Field[Seq[R], MM]
  ): SpindleEmbeddedRecordListModifyField[R, MM] = new SpindleEmbeddedRecordListModifyField(f)
}

object SpindleRogue extends SpindleRogue