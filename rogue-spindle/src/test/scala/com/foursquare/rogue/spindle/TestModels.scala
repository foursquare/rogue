// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.spindle.UntypedMetaRecord
import com.mongodb.{DB, MongoClient, ServerAddress}

class TestDatabaseService extends SpindleDatabaseService(new TestDBCollectionFactory()) {
  def disconnectFromMongo() {
    dbCollectionFactory.asInstanceOf[TestDBCollectionFactory].disconnectFromMongo()
  }
}

class TestDBCollectionFactory extends SpindleDBCollectionFactory {
  val mongoClient = new MongoClient(new ServerAddress("localhost", 37648))

  override def getPrimaryDB(meta: UntypedMetaRecord): DB = {
    val identifierStr = getIdentifier(meta)
    if (identifierStr != "rogue_mongo") {
      throw new Exception("The mongo_identifier annotation in the Thrift definition must be rogue_mongo for these tests.")
    }
    mongoClient.getDB(identifierStr)
  }
  override val indexCache = None

  def disconnectFromMongo() {
    mongoClient.close()
  }
}

// Used for selectCase tests.
case class V1(legacyid: Option[Long])
case class V2(legacyid: Option[Long], userid: Option[Long])
case class V3(legacyid: Option[Long], userid: Option[Long], mayor: Option[Long])
case class V4(legacyid: Option[Long], userid: Option[Long], mayor: Option[Long], mayor_count: Option[Int])
case class V5(legacyid: Option[Long], userid: Option[Long], mayor: Option[Long], mayor_count: Option[Int], closed: Option[Boolean])
case class V6(legacyid: Option[Long], userid: Option[Long], mayor: Option[Long], mayor_count: Option[Int], closed: Option[Boolean], tags: Option[Seq[String]])
