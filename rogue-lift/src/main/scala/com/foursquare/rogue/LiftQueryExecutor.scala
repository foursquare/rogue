// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.MongoSelect
import net.liftweb.common.{Box, Full}
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import org.bson.types.BasicBSONList
import net.liftweb.mongodb.MongoDB
import com.mongodb.{DBCollection, DBObject, ReadPreference}

object LiftDBCollectionFactory extends DBCollectionFactory[MongoRecord[_] with MongoMetaRecord[_]] {
  override def getDBCollection[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): DBCollection = {
    MongoDB.useSession(query.meta.mongoIdentifier){ db =>
      db.getCollection(query.collectionName)
    }
  }
  override def getPrimaryDBCollection[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): DBCollection = {
    MongoDB.useSession(query.meta/* TODO: .master*/.mongoIdentifier){ db =>
      db.getCollection(query.collectionName)
    }
  }
  override def getInstanceName[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): String = {
    query.meta.mongoIdentifier.toString
  }
}

class LiftAdapter(dbCollectionFactory: DBCollectionFactory[MongoRecord[_] with MongoMetaRecord[_]])
  extends MongoJavaDriverAdapter(dbCollectionFactory)

object LiftAdapter extends LiftAdapter(LiftDBCollectionFactory)

class LiftQueryExecutor(override val adapter: MongoJavaDriverAdapter[MongoRecord[_] with MongoMetaRecord[_]]) extends QueryExecutor[MongoRecord[_] with MongoMetaRecord[_]] {
  override def defaultWriteConcern = QueryHelpers.config.defaultWriteConcern
  override def defaultReadPreference = ReadPreference.secondary

  override protected def serializer[M <: MongoRecord[_] with MongoMetaRecord[_], R](
      meta: M,
      select: Option[MongoSelect[M, R]]
  ): RogueSerializer[R] = {
    new RogueSerializer[R] {
      override def fromDBObject(dbo: DBObject): R = select match {
        case Some(MongoSelect(Nil, transformer)) =>
          // A MongoSelect clause exists, but has empty fields. Return null.
          // This is used for .exists(), where we just want to check the number
          // of returned results is > 0.
          transformer(null)
        case Some(MongoSelect(fields, transformer)) =>
          val inst = meta.createRecord.asInstanceOf[MongoRecord[_]]

          LiftQueryExecutorHelpers.setInstanceFieldFromDbo(inst, dbo, "_id")

          val values =
            fields.map(fld => {
              val valueOpt = LiftQueryExecutorHelpers.setInstanceFieldFromDbo(inst, dbo, fld.field.name)
              fld.valueOrDefault(valueOpt)
            })

          transformer(values)
        case None =>
          meta.fromDBObject(dbo).asInstanceOf[R]
      }
    }
  }
}

object LiftQueryExecutor extends LiftQueryExecutor(LiftAdapter)

object LiftQueryExecutorHelpers {
  def setInstanceFieldFromDbo(instance: MongoRecord[_], dbo: DBObject, fieldName: String): Option[_] = {
    instance.fieldByName(fieldName) match {
      case Full(field) => field.setFromAny(dbo.get(fieldName)).toOption
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
        })).toOption
      }
    }
  }
}
