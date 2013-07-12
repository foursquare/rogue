// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import net.liftweb.mongodb.record.MongoRecord

trait HasMongoForeignObjectId[RefType <: MongoRecord[RefType] with ObjectIdKey[RefType]]
