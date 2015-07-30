// Copyright 2015 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.spindle.{UntypedMetaRecord, UntypedRecord}
import com.mongodb.{BasicDBObject, DBCursor, DBCallback, DBCollection, DBDecoder, DBDecoderFactory, DBObject}
import java.io.InputStream
import org.bson.{BasicBSONDecoder, BSONCallback, BSONObject}

case class SpindleDBDecoderFactory(
  meta: UntypedMetaRecord,
  recordReader: (UntypedMetaRecord, InputStream) => SpindleDBObject
) extends DBDecoderFactory {
  def create(): DBDecoder = new SpindleDBDecoder(meta, recordReader)
}

case class SpindleDBDecoder(
  meta: UntypedMetaRecord,
  recordReader: (UntypedMetaRecord, InputStream) => SpindleDBObject
) extends DBDecoder {
  def decode(is: InputStream, collection: DBCollection): DBObject = {
    recordReader(meta, is)
  }

  def getDBCallback(collection: DBCollection): DBCallback = {
    throw new UnsupportedOperationException()
  }
  def decode(bytes: Array[Byte], collection: DBCollection): DBObject = {
    throw new UnsupportedOperationException()
  }
  def readObject(bytes: Array[Byte]): BSONObject = {
    throw new UnsupportedOperationException()
  }
  def readObject(in: InputStream): BSONObject = {
    throw new UnsupportedOperationException()
  }
  def decode(bytes: Array[Byte], callback: BSONCallback): Int = {
    throw new UnsupportedOperationException()
  }
  def decode(in: InputStream, callback: BSONCallback): Int = {
    throw new UnsupportedOperationException()
  }
}

trait SpindleDBObject extends DBObject {
  def record: UntypedRecord
  override def isPartialObject(): Boolean = false

  override def put(key: String, dv: Object): Object = throw new UnsupportedOperationException()
  override def putAll(o: BSONObject): Unit = throw new UnsupportedOperationException()
  override def putAll(m: java.util.Map[_, _]): Unit = throw new UnsupportedOperationException()
  override def toMap(): java.util.Map[_, _] = throw new UnsupportedOperationException()
  override def removeField(key: String): Object = throw new UnsupportedOperationException()
  override def containsKey(s: String): Boolean = throw new UnsupportedOperationException()
  override def containsField(s: String): Boolean = throw new UnsupportedOperationException()
  override def keySet(): java.util.Set[String] = throw new UnsupportedOperationException()
}

case class SpindleNativeDBObject(record: UntypedRecord, errorMessage: String, code: Int) extends SpindleDBObject {
  override def markAsPartialObject(): Unit = {}
  override def get(key: String): Object = {
    if ("$err" == key) {
      errorMessage
    } else if ("code" == key) {
      code: java.lang.Integer
    } else {
      null
    }
  }
}

case class SpindleMongoDBObject(record: UntypedRecord, dbo: DBObject) extends SpindleDBObject {
  override def markAsPartialObject(): Unit = {
    dbo.markAsPartialObject()
  }
  override def get(key: String): Object = dbo.get(key)
}
