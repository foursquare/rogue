// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.lift

import com.foursquare.index.{IndexedRecord, UntypedMongoIndex}
import com.foursquare.rogue.{DBCollectionFactory, MongoJavaDriverAdapter, Query, QueryExecutor, QueryHelpers,
    QueryOptimizer, RogueReadSerializer, RogueWriteSerializer}
import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.mongodb.{DBCollection, DBObject}
import net.liftweb.common.{Box, Full}
import net.liftweb.mongodb.record.{BsonRecord, BsonMetaRecord, MongoRecord, MongoMetaRecord}
import net.liftweb.mongodb.MongoDB
import net.liftweb.mongodb.record.field.BsonRecordField
import net.liftweb.record.Record
import org.bson.types.BasicBSONList
import sun.reflect.generics.reflectiveObjects.NotImplementedException

object LiftDBCollectionFactory extends DBCollectionFactory[MongoRecord[_] with MongoMetaRecord[_], MongoRecord[_]] {
  override def getDBCollection[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): DBCollection = {
    MongoDB.useSession(query.meta.mongoIdentifier){ db =>
      db.getCollection(query.collectionName)
    }
  }
  protected def getPrimaryDBCollection(meta: MongoMetaRecord[_], collectionName: String): DBCollection = {
    MongoDB.useSession(meta/* TODO: .master*/.mongoIdentifier){ db =>
      db.getCollection(collectionName)
    }    
  }
  override def getPrimaryDBCollection[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): DBCollection = {
    getPrimaryDBCollection(query.meta, query.collectionName)
  }
  override def getPrimaryDBCollection(record: MongoRecord[_]): DBCollection = {
    getPrimaryDBCollection(record.meta, record.meta.collectionName)
  }
  override def getInstanceName[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): String = {
    query.meta.mongoIdentifier.toString
  }

  /**
   * Retrieves the list of indexes declared for the record type associated with a
   * query. If the record type doesn't declare any indexes, then returns None.
   * @param query the query
   * @return the list of indexes, or an empty list.
   */
  override def getIndexes[M <: MongoRecord[_] with MongoMetaRecord[_]](query: Query[M, _, _]): Option[List[UntypedMongoIndex]] = {
    val queryMetaRecord = query.meta
    if (queryMetaRecord.isInstanceOf[IndexedRecord[_]]) {
      Some(queryMetaRecord.asInstanceOf[IndexedRecord[_]].mongoIndexList)
    } else {
      None
    }
  }
}

class LiftAdapter(dbCollectionFactory: DBCollectionFactory[MongoRecord[_] with MongoMetaRecord[_], MongoRecord[_]])
  extends MongoJavaDriverAdapter(dbCollectionFactory)

object LiftAdapter extends LiftAdapter(LiftDBCollectionFactory)

class LiftQueryExecutor(
    override val adapter: MongoJavaDriverAdapter[MongoRecord[_] with MongoMetaRecord[_], MongoRecord[_]]
) extends QueryExecutor[MongoRecord[_] with MongoMetaRecord[_], MongoRecord[_]] {
  override def defaultWriteConcern = QueryHelpers.config.defaultWriteConcern
  override lazy val optimizer = new QueryOptimizer

  override protected def readSerializer[M <: MongoRecord[_] with MongoMetaRecord[_], R](
      meta: M,
      select: Option[MongoSelect[M, R]]
  ): RogueReadSerializer[R] = {
    new RogueReadSerializer[R] {
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

  override protected def writeSerializer(record: MongoRecord[_]): RogueWriteSerializer[MongoRecord[_]] = {
    new RogueWriteSerializer[MongoRecord[_]] {
      override def toDBObject(record: MongoRecord[_]): DBObject = {
        record.asDBObject
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
        val names = fieldName.split("\\.").toList.filter(_ != "$")
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
