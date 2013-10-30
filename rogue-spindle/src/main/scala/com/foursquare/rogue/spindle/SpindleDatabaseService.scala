// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.rogue.{DBCollectionFactory, MongoJavaDriverAdapter, QueryExecutor, QueryOptimizer}
import com.foursquare.spindle.{UntypedMetaRecord, UntypedRecord}
import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.mongodb.WriteConcern

class SpindleDatabaseService(val dbCollectionFactory: SpindleDBCollectionFactory) extends QueryExecutor[UntypedMetaRecord, UntypedRecord] {
  override def readSerializer[M <: UntypedMetaRecord, R](meta: M, select: Option[MongoSelect[M, R]]):
      SpindleRogueReadSerializer[M, R] = {
    new SpindleRogueReadSerializer(meta, select)
  }
  override def writeSerializer(record: UntypedRecord): SpindleRogueWriteSerializer = new SpindleRogueWriteSerializer
  override def defaultWriteConcern: WriteConcern = WriteConcern.SAFE
  override val adapter: MongoJavaDriverAdapter[UntypedMetaRecord, UntypedRecord] = new MongoJavaDriverAdapter(dbCollectionFactory)
  override val optimizer = new QueryOptimizer

  override def save[R <: UntypedRecord](record: R, writeConcern: WriteConcern = defaultWriteConcern): R = {
    if (record.meta.annotations.contains("nosave"))
      throw new IllegalArgumentException("Cannot save a %s record".format(record.meta.recordName))
    super.save(record, writeConcern)
  }

  override def insert[R <: UntypedRecord](record: R, writeConcern: WriteConcern = defaultWriteConcern): R = {
    if (record.meta.annotations.contains("nosave"))
      throw new IllegalArgumentException("Cannot insert a %s record".format(record.meta.recordName))
    super.insert(record, writeConcern)
  }
}
