// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.rogue.{InitialState, Query => RogueQuery}
import com.foursquare.rogue.MongoHelpers.AndCondition
import com.foursquare.spindle.{MetaRecord, Record}

object SpindleQuery {
  def apply[R <: Record[R], M <: MetaRecord[R, M]](
      model: M with MetaRecord[R, M]
  ): RogueQuery[M, R, InitialState] = {
    val collection = model.annotations.get("mongo_collection").getOrElse {
      throw new Exception("Add a mongo_collection annotation to the Thrift definition for this class.")
    }
    RogueQuery[M, R, InitialState](
      model, collection, None, None, None, None, None, AndCondition(Nil, None), None, None, None)
  }
}
