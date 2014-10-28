// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.common.thrift.bson.TBSONObjectProtocol
import com.foursquare.field.Field
import com.foursquare.spindle.{Enum, CompanionProvider, Record, MetaRecord}
import com.foursquare.rogue.{AbstractListModifyField, AbstractListQueryField, AbstractModifyField, AbstractQueryField, SelectableDummyField}
import org.apache.thrift.TBase
import org.bson.BSONObject

class SpindleEnumQueryField[M, E <: Enum[E]](field: Field[E, M])
    extends AbstractQueryField[E, E, Int, M](field) {
  override def valueToDB(e: E) = e.id
}

class SpindleEnumListQueryField[M, E <: Enum[E]](field: Field[Seq[E], M])
    extends AbstractListQueryField[E, E, Int, M, Seq](field) {
  override def valueToDB(e: E) = e.id
}

class SpindleEnumModifyField[M, E <: Enum[E]](field: Field[E, M])
    extends AbstractModifyField[E, Int, M](field) {
  override def valueToDB(e: E) = e.id
}

class SpindleEnumListModifyField[M, E <: Enum[E]](field: Field[Seq[E], M])
    extends AbstractListModifyField[E, Int, M, Seq](field) {
  override def valueToDB(e: E) = e.id
}

abstract class SpindleEmbeddedRecordQueryFieldHelper[C, F1, F2] {
  def field(subfield: C => F1): F2
  def select(subfield: C => F1): F2 = field(subfield)
}

abstract class SpindleEmbeddedRecordListQueryFieldHelper[C, F1, F2, F3] {
  def field(subfield: C => F1): F2
  def select(subfield: C => F1): F3
}

class SpindleEmbeddedRecordQueryField[
    R <: Record[_],
    MM <: MetaRecord[_, _]
](
    f: Field[R, MM]
) extends AbstractQueryField[R, R, BSONObject, MM](f) {

  override def valueToDB(b: R) = {
    val factory = new TBSONObjectProtocol.WriterFactoryForDBObject
    val protocol = factory.getProtocol
    b.asInstanceOf[TBase[_, _]].write(protocol)
    protocol.getOutput
  }

  def sub[
      RR <: Record[RR],
      V
  ](implicit
      ev: R <:< Record[RR],
      d: CompanionProvider[RR]
  ) = new SpindleEmbeddedRecordQueryFieldHelper[d.CompanionT, Field[V, d.CompanionT], SelectableDummyField[V, MM]] {
    override def field(subfield: d.CompanionT => Field[V, d.CompanionT]): SelectableDummyField[V, MM] = {
      new SelectableDummyField[V, MM](f.name + "." + subfield(d.provide).name, f.owner)
    }
  }

  def unsafeField[V](name: String): SelectableDummyField[V, MM] = {
    new SelectableDummyField[V, MM](f.name + "." + name, f.owner)
  }
}

class SpindleEmbeddedRecordModifyField[
    R <: Record[_],
    MM <: MetaRecord[_, _]
](
    f: Field[R, MM]
) extends AbstractModifyField[R, BSONObject, MM](f) {

  override def valueToDB(b: R) = {
    val factory = new TBSONObjectProtocol.WriterFactoryForDBObject
    val protocol = factory.getProtocol
    b.asInstanceOf[TBase[_, _]].write(protocol)
    protocol.getOutput
  }
}


class SpindleEmbeddedRecordListQueryField[
    R <: Record[_],
    MM <: MetaRecord[_, _]
](
    f: Field[Seq[R], MM]
) extends AbstractListQueryField[R, R, BSONObject, MM, Seq](f) {
  override def valueToDB(b: R) = {
    val factory = new TBSONObjectProtocol.WriterFactoryForDBObject
    val protocol = factory.getProtocol
    b.asInstanceOf[TBase[_, _]].write(protocol)
    protocol.getOutput
  }

  def sub[
      RR <: Record[RR],
      V
  ](implicit
      ev: R <:< Record[RR],
      d: CompanionProvider[RR]
  ) = new SpindleEmbeddedRecordListQueryFieldHelper[d.CompanionT, Field[V, d.CompanionT], SelectableDummyField[V, MM], SelectableDummyField[Seq[Option[V]], MM]] {
    override def field(subfield: d.CompanionT => Field[V, d.CompanionT]): SelectableDummyField[V, MM] = {
      new SelectableDummyField[V, MM](f.name + "." + subfield(d.provide).name, f.owner)
    }
    override def select(subfield: d.CompanionT => Field[V, d.CompanionT]): SelectableDummyField[Seq[Option[V]], MM] = {
      new SelectableDummyField[Seq[Option[V]], MM](f.name + "." + subfield(d.provide).name, f.owner)
    }
  }

  def unsafeField[V](name: String): SelectableDummyField[Seq[V], MM] = {
    new SelectableDummyField[Seq[V], MM](f.name + "." + name, f.owner)
  }
}

class SpindleEmbeddedRecordListModifyField[
    R <: Record[_],
    MM <: MetaRecord[_, _]
](
    f: Field[Seq[R], MM]
) extends AbstractListModifyField[R, BSONObject, MM, Seq](f) {

  override def valueToDB(b: R) = {
    val factory = new TBSONObjectProtocol.WriterFactoryForDBObject
    val protocol = factory.getProtocol
    b.asInstanceOf[TBase[_, _]].write(protocol)
    protocol.getOutput
  }
}
