// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

import com.foursquare.rogue.LiftRogue._
import com.foursquare.rogue.MongoHelpers.AndCondition
import org.junit._
import org.specs2.matcher.JUnitMustMatchers
import net.liftweb.mongodb.record.{MongoMetaRecord, MongoRecord}

class LegacyQueryExecutorTest extends JUnitMustMatchers {

  @Test
  def testExeptionInRunCommandIsDecorated {
    val query = Venue.where(_.tags contains "test").asInstanceOf[Query[MongoRecord[_] with MongoMetaRecord[_], _, _]]
    (LiftAdapter.runCommand("hello", query){
      throw new RuntimeException("bang")
      "hi"
    }) must throwA[RogueException]
  }

}
