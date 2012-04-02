// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.recordv2.Field
import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.mongodb.{DBObject, ReadPreference, WriteConcern}
import net.liftweb.common.{Box, Full}
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import org.bson.types.BasicBSONList

object ConcreteLiftQueryExecutor extends ConcreteLiftQueryExecutor

class ConcreteLiftQueryExecutor extends LiftQueryExecutor {
  override def defaultWriteConcern = QueryHelpers.config.defaultWriteConcern
  override def defaultReadPreference = ReadPreference.PRIMARY

  override protected def serializer[M <: MongoRecord[_] with MongoMetaRecord[_], R](
      meta: M,
      select: Option[MongoSelect[R]]
  ): LiftRogueSerializer[R] = {
    new LiftRogueSerializer[R] {
      override def fromDBObject(dbo: DBObject): R = select match {
        case Some(MongoSelect(Nil, transformer)) =>
          // A MongoSelect clause exists, but has empty fields. Return null.
          // This is used for .exists(), where we just want to check the number
          // of returned results is > 0.
          transformer(null)
        case Some(MongoSelect(fields, transformer)) =>
          val inst = meta.createRecord.asInstanceOf[MongoRecord[_]]

          def setInstanceFieldFromDbo(fieldName: String) = {
            inst.fieldByName(fieldName) match {
              case Full(fld) => fld.setFromAny(dbo.get(fieldName))
              case _ => {
                val splitName = fieldName.split('.').toList
                Box.!!(splitName.foldLeft(dbo: Object)((obj: Object, fieldName: String) => {
                  obj match {
                    case dbl: BasicBSONList =>
                      (for {
                        index <- 0 to dbl.size - 1
                        val item: DBObject = dbl.get(index).asInstanceOf[DBObject]
                      } yield item.get(fieldName)).toList
                    case dbo: DBObject =>
                      dbo.get(fieldName)
                    case null => null
                  }
                }))
              }
            }
          }

          setInstanceFieldFromDbo("_id")
          transformer(fields.map(fld => fld(setInstanceFieldFromDbo(fld.field.name))))
        case None =>
          meta.fromDBObject(dbo).asInstanceOf[R]
      }
    }
  }
}
