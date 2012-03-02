// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import net.liftweb.mongodb.record.{MongoId, MongoRecord}

trait HasMongoForeignObjectId[RefType <: MongoRecord[RefType] with MongoId[RefType]]
