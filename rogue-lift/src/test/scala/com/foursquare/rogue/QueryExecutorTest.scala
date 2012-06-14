// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

import org.junit._
import org.specs.SpecsMatchers
import com.foursquare.rogue.MongoHelpers.AndCondition
import net.liftweb.mongodb.record.{MongoId, MongoRecord, MongoMetaRecord}

class LegacyQueryExecutorTest extends SpecsMatchers {

  class Dummy extends MongoRecord[Dummy] with MongoId[Dummy] {
    def meta = Dummy
  }

  object Dummy extends Dummy with MongoMetaRecord[Dummy] {
  }

  @Test
  def testExeptionInRunCommandIsDecorated {
    val query = Query[Dummy.type, Dummy, InitialState](
      Dummy, "Dummy", None, None, None, None, None, AndCondition(Nil, None), None, None, None)
    (LiftAdapter.runCommand("hello", query){
      throw new RuntimeException("bang")
      "hi"
    }) must throwA[RogueException]
  }

}
