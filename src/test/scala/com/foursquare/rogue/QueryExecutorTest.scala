// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.QueryExecutor

import org.junit._
import org.specs.SpecsMatchers

class QueryExecutorTest extends SpecsMatchers {
  
  @Test
  def testExeptionInRunCommandIsDecorated {
    (QueryExecutor.runCommand("hello","someid"){
      throw new RuntimeException("bang")
      "hi"
    }) must throwA[RogueException]
  }
  
}