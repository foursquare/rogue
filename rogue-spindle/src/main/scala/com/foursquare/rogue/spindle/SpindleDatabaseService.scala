// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.rogue.{DBCollectionFactory, MongoJavaDriverAdapter, QueryExecutor, QueryOptimizer}
import com.foursquare.spindle.{UntypedMetaRecord, UntypedRecord}
import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.mongodb.WriteConcern

class SpindleDatabaseService(val dbCollectionFactory: SpindleDBCollectionFactory) extends QueryExecutor[UntypedMetaRecord] {
  override def serializer[M <: UntypedMetaRecord, R](meta: M, select: Option[MongoSelect[M, R]]):
      SpindleRogueSerializer[M, R] = {
    new SpindleRogueSerializer(meta, select)
  }
  override def defaultWriteConcern: WriteConcern = WriteConcern.SAFE
  override val adapter: MongoJavaDriverAdapter[UntypedMetaRecord] = new MongoJavaDriverAdapter(dbCollectionFactory)
  override val optimizer = new QueryOptimizer

  def save[R <: UntypedRecord](record: R, writeConcern: WriteConcern = defaultWriteConcern): R = {
    if (record.meta.annotations.contains("nosave"))
      throw new IllegalArgumentException("Cannot save a %s record".format(record.meta.recordName))
    val s = serializer[UntypedMetaRecord, R](record.meta, None)
    val dbo = s.toDBObject(record)
    val collection = dbCollectionFactory.getPrimaryDBCollection(record.meta)
    collection.save(dbo, writeConcern)
    record
  }
}
