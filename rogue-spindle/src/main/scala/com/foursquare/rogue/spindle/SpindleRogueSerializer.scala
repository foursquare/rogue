// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.common.thrift.bson.TBSONObjectProtocol
import com.foursquare.rogue.MongoHelpers.MongoSelect
import com.foursquare.rogue.{RogueReadSerializer, RogueWriteSerializer, SelectField}
import com.foursquare.spindle.{UntypedFieldDescriptor, UntypedMetaRecord, UntypedRecord}
import com.mongodb.DBObject

class SpindleRogueReadSerializer[M <: UntypedMetaRecord, R](meta: M, select: Option[MongoSelect[M, R]])
    extends RogueReadSerializer[R] {

  private def getValueFromRecord(metaRecord: UntypedMetaRecord, record: Any, fieldName: String): Option[Any] = {
    val fieldList: List[UntypedFieldDescriptor] = metaRecord.untypedFields.toList
    val fieldDescriptor = fieldList.find(fd => fd.name == fieldName).getOrElse(
      throw new Exception("The meta record does not have a definition for field %s".format(fieldName))
    )
    fieldDescriptor.unsafeGetterOption(record)
  }

  private def getValueFromAny(sourceObj: Option[Any], fieldName: String): Option[Any] = sourceObj.flatMap(_ match {
    case (map: Map[_, _]) => map.find({case(key, value) => key.toString == fieldName}).map(_._2)
    case (seq: Seq[_]) => Some(seq.map(v => getValueFromAny(Some(v), fieldName)))
    case (rec: UntypedRecord) => getValueFromRecord(rec.meta, rec, fieldName)
    case _ => throw new Exception("Rogue bug: unepected object type")
  })

  private def extractRecordFromDbo(dbo: DBObject): R = {
    dbo match {
      case sdbo: SpindleDBObject => sdbo.record.asInstanceOf[R]
      case _ =>
        val record = meta.createUntypedRawRecord
        val protocolFactory = new TBSONObjectProtocol.ReaderFactory
        val protocol = protocolFactory.getProtocol
        protocol.setSource(dbo)
        record.read(protocol)
        record.asInstanceOf[R]
    }
  }

  override def fromDBObject(dbo: DBObject): R = select match {
    case Some(MongoSelect(Nil, transformer)) => {
      // A MongoSelect clause exists, but has empty fields. Return null.
      // This is used for .exists(), where we just want to check the number
      // of returned results is > 0.
      transformer(null)
    }
    case Some(MongoSelect(fields, transformer)) => {
      val record: R = extractRecordFromDbo(dbo)

      val values = {
        fields.map(fld => {
          if (fld.field.isInstanceOf[UntypedFieldDescriptor]) {
            val valueOpt = fld.field.asInstanceOf[UntypedFieldDescriptor].unsafeGetterOption(record)
            fld.valueOrDefault(valueOpt)
          } else {
            // We need to handle a request for a subrecord, such as foo.x.y
            val (rootFieldName :: subPath) = fld.field.name.split('.').toList.filter(_ != "$")
            val rootValueOpt = getValueFromRecord(fld.field.owner.asInstanceOf[UntypedMetaRecord], record, rootFieldName)
            val valueOpt = subPath.foldLeft(rootValueOpt)(getValueFromAny)
            fld.valueOrDefault(valueOpt)
          }
        })
      }
      transformer(values)
    }
    case None => {
      extractRecordFromDbo(dbo)
    }
  }
}

class SpindleRogueWriteSerializer extends RogueWriteSerializer[UntypedRecord] {
  def toDBObject(record: UntypedRecord): DBObject = {
    val protocolFactory = new TBSONObjectProtocol.WriterFactoryForDBObject
    val protocol = protocolFactory.getProtocol
    record.write(protocol)
    protocol.getOutput.asInstanceOf[DBObject]
  }
}
