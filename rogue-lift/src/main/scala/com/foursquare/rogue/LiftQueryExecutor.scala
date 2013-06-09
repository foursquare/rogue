// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.MongoSelect
import net.liftweb.common.{Box, Full}
import net.liftweb.mongodb.record.{BsonRecord, BsonMetaRecord, MongoRecord, MongoMetaRecord}
import org.bson.types.BasicBSONList
import net.liftweb.mongodb.MongoDB
import com.mongodb.{DBCollection, DBObject}
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import net.liftweb.mongodb.record.field.BsonRecordField
import net.liftweb.record.Record

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
  override lazy val optimizer = new QueryOptimizer

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
  import net.liftweb.record.{Field => LField}

  def setInstanceFieldFromDboList(instance: BsonRecord[_], dbo: DBObject, fieldNames: List[String]): Option[_] = {
    fieldNames match {
      case last :: Nil =>
        val fld: Box[LField[_, _]] = instance.fieldByName(last)
        fld.flatMap(setLastFieldFromDbo(_, dbo, last))
      case name :: rest =>
        val fld: Box[LField[_, _]] = instance.fieldByName(name)
        dbo.get(name) match {
          case obj: DBObject => fld.flatMap(setFieldFromDbo(_, obj, rest))
          case list: BasicBSONList => fallbackValueFromDbObject(dbo, fieldNames)
          case null => None
      }
      case Nil => throw new UnsupportedOperationException("was called with empty list, shouldn't possibly happen")
    }
  }

  def setFieldFromDbo(field: LField[_, _], dbo: DBObject, fieldNames: List[String]): Option[_] = {
    if (field.isInstanceOf[BsonRecordField[_, _]]) {
      val brf = field.asInstanceOf[BsonRecordField[_, _]]
      val inner = brf.value.asInstanceOf[BsonRecord[_]]
      setInstanceFieldFromDboList(inner, dbo, fieldNames)
    } else {
      fallbackValueFromDbObject(dbo, fieldNames)
    }
  }

  def setLastFieldFromDbo(field: LField[_, _], dbo: DBObject, fieldName: String): Option[_] = {
    field.setFromAny(dbo.get(fieldName)).toOption
  }

  def setInstanceFieldFromDbo(instance: MongoRecord[_], dbo: DBObject, fieldName: String): Option[_] = {
    fieldName.contains(".") match {
      case true =>
        val names = fieldName.split("\\.").toList
        setInstanceFieldFromDboList(instance, dbo, names)
      case false =>
        val fld: Box[LField[_, _]] = instance.fieldByName(fieldName)
        fld.flatMap (setLastFieldFromDbo(_, dbo, fieldName))
    }
  }

  def fallbackValueFromDbObject(dbo: DBObject, fieldNames: List[String]): Option[_] = {
    import scala.collection.JavaConversions._
    Box.!!(fieldNames.foldLeft(dbo: Object)((obj: Object, fieldName: String) => {
      obj match {
        case dbl: BasicBSONList =>
          dbl.map(_.asInstanceOf[DBObject]).map(_.get(fieldName)).toList
        case dbo: DBObject =>
          dbo.get(fieldName)
        case null => null
      }
    })).toOption
  }
}
