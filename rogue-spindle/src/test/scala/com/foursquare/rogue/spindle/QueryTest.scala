// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.rogue.spindle.gen.{ThriftClaimStatus, ThriftComment, ThriftConsumerPrivilege, ThriftLike,
    ThriftOAuthConsumer, ThriftRejectReason, ThriftSourceBson, ThriftTip, ThriftVenue, ThriftVenueClaim,
    ThriftVenueClaimBson, ThriftVenueMeta, ThriftVenueStatus}
import com.foursquare.field.Field
import com.foursquare.rogue.{BSONType, MongoType, Query, QueryField, QueryOptimizer}
import com.foursquare.rogue.spindle.gen.IdsTypedefs.VenueId
import com.foursquare.rogue.spindle.SpindleRogue._
import com.foursquare.spindle.{MetaRecord, Record}
import com.mongodb.ReadPreference
import java.util.regex.Pattern
import org.bson.types.ObjectId
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.{Ignore, Test}
import org.specs2.matcher.JUnitMustMatchers

class QueryTest extends JUnitMustMatchers {

  val Q = SpindleQuery

  @Test
  def testProduceACorrectJSONQueryString {

    val d1 = new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)
    val d2 = new DateTime(2010, 5, 2, 0, 0, 0, 0, DateTimeZone.UTC)
    val oid1 = new ObjectId(d1.toDate, 0, 0)
    val oid2 = new ObjectId(d2.toDate, 0, 0)
    val oid = new ObjectId
    val ven1 = ThriftVenue.newBuilder.id(VenueId(oid1)).result

    // eqs
    Q(ThriftVenue).where(_.mayor eqs 1)              .toString() must_== """db.venues.find({ "mayor" : 1})"""
    Q(ThriftVenue).where(_.venuename eqs "Starbucks").toString() must_== """db.venues.find({ "venuename" : "Starbucks"})"""
    Q(ThriftVenue).where(_.closed eqs true)          .toString() must_== """db.venues.find({ "closed" : true})"""
    Q(ThriftVenue).where(_.id eqs VenueId(oid)) .toString() must_== ("""db.venues.find({ "_id" : ObjectId("%s")})""" format oid.toString)
    /* TODO(rogue-named-enums)
    Q(ThriftVenueClaim).where(_.status eqs ThriftClaimStatus.approved).toString() must_== """db.venueclaims.find({ "status" : "Approved"})"""
    */

    Q(ThriftVenueClaim).where(_.venueId eqs VenueId(oid))     .toString() must_== ("""db.venueclaims.find({ "vid" : ObjectId("%s")})""" format oid.toString)
    Q(ThriftVenueClaim).where(_.venueId eqs ven1.id) .toString() must_== ("""db.venueclaims.find({ "vid" : ObjectId("%s")})""" format oid1.toString)
    /* TODO(rogue-foreign-key-support)
    Q(ThriftVenueClaim).where(_.venueId eqs ven1)    .toString() must_== ("""db.venueclaims.find({ "vid" : ObjectId("%s")})""" format oid1.toString)
    */

    // neq,lt,gt
    Q(ThriftVenue).where(_.mayor_count neqs 5).toString() must_== """db.venues.find({ "mayor_count" : { "$ne" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count < 5)   .toString() must_== """db.venues.find({ "mayor_count" : { "$lt" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count lt 5)  .toString() must_== """db.venues.find({ "mayor_count" : { "$lt" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count <= 5)  .toString() must_== """db.venues.find({ "mayor_count" : { "$lte" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count lte 5) .toString() must_== """db.venues.find({ "mayor_count" : { "$lte" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count > 5)   .toString() must_== """db.venues.find({ "mayor_count" : { "$gt" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count gt 5)  .toString() must_== """db.venues.find({ "mayor_count" : { "$gt" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count >= 5)  .toString() must_== """db.venues.find({ "mayor_count" : { "$gte" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count gte 5) .toString() must_== """db.venues.find({ "mayor_count" : { "$gte" : 5}})"""
    Q(ThriftVenue).where(_.mayor_count between (3, 5)).toString() must_== """db.venues.find({ "mayor_count" : { "$gte" : 3 , "$lte" : 5}})"""
    Q(ThriftVenue).where(_.popularity < 4).toString()     must_== """db.venues.find({ "popularity" : { "$lt" : 4}})"""
    /* TODO(rogue-named-enums)
    Q(ThriftVenueClaim).where(_.status neqs ThriftClaimStatus.approved).toString() must_== """db.venueclaims.find({ "status" : { "$ne" : "Approved"}})"""
    */
    Q(ThriftVenueClaim).where(_.reason eqs ThriftRejectReason.tooManyClaims).toString() must_== """db.venueclaims.find({ "reason" : 0})"""
    Q(ThriftVenueClaim).where(_.reason eqs ThriftRejectReason.cheater).toString() must_== """db.venueclaims.find({ "reason" : 1})"""
    Q(ThriftVenueClaim).where(_.reason eqs ThriftRejectReason.wrongCode).toString() must_== """db.venueclaims.find({ "reason" : 2})"""

    // Comparison operators on arbitrary fields.
    // Warning: arbitraryFieldToQueryField is dangerous if T is not a serializable type by DBObject
    def arbitraryFieldToQueryField[T: BSONType, M](f: Field[T, M]): QueryField[T, M] = new QueryField(f)
    def doLessThan[R <: Record[R], M <: MetaRecord[R, M], T: BSONType](meta: M, f: M => Field[T, M], otherVal: T) = {
      Q(meta).where(r => arbitraryFieldToQueryField(f(r)) < otherVal)
    }
    doLessThan[ThriftVenue, ThriftVenueMeta, Int](ThriftVenue, _.mayor_count, 5).toString() must_== """db.venues.find({ "mayor_count" : { "$lt" : 5}})"""

    // in,nin
    Q(ThriftVenue).where(_.legacyid in List(123L, 456L)).toString() must_== """db.venues.find({ "legid" : { "$in" : [ 123 , 456]}})"""
    Q(ThriftVenue).where(_.venuename nin List("Starbucks", "Whole Foods")).toString() must_== """db.venues.find({ "venuename" : { "$nin" : [ "Starbucks" , "Whole Foods"]}})"""
    /* TODO(rogue-named-enums)
    Q(ThriftVenueClaim).where(_.status in List(ThriftClaimStatus.approved, ThriftClaimStatus.pending)) .toString() must_== """db.venueclaims.find({ "status" : { "$in" : [ "Approved" , "Pending approval"]}})"""
    Q(ThriftVenueClaim).where(_.status nin List(ThriftClaimStatus.approved, ThriftClaimStatus.pending)).toString() must_== """db.venueclaims.find({ "status" : { "$nin" : [ "Approved" , "Pending approval"]}})"""
    */

    Q(ThriftVenueClaim).where(_.venueId in List(ven1.id)) .toString() must_== ("""db.venueclaims.find({ "vid" : { "$in" : [ ObjectId("%s")]}})""" format oid1.toString)
    /* TODO(rogue-foreign-key-support)
    Q(ThriftVenueClaim).where(_.venueId in List(ven1))    .toString() must_== ("""db.venueclaims.find({ "vid" : { "$in" : [ ObjectId("%s")]}})""" format oid1.toString)
    */

    Q(ThriftVenueClaim).where(_.venueId nin List(ven1.id))  .toString() must_== ("""db.venueclaims.find({ "vid" : { "$nin" : [ ObjectId("%s")]}})""" format oid1.toString)
    /* TODO(rogue-foreign-key-support)
    Q(ThriftVenueClaim).where(_.venueId nin List(ven1))     .toString() must_== ("""db.venueclaims.find({ "vid" : { "$nin" : [ ObjectId("%s")]}})""" format oid1.toString)
    */


    // exists
    Q(ThriftVenue).where(_.id exists true).toString() must_== """db.venues.find({ "_id" : { "$exists" : true}})"""

    // startsWith, regex
    Q(ThriftVenue).where(_.venuename startsWith "Starbucks").toString() must_== """db.venues.find({ "venuename" : { "$regex" : "^\\QStarbucks\\E" , "$options" : ""}})"""
    val p1 = Pattern.compile("Star.*")
    Q(ThriftVenue).where(_.venuename regexWarningNotIndexed p1).toString() must_== """db.venues.find({ "venuename" : { "$regex" : "Star.*" , "$options" : ""}})"""
    Q(ThriftVenue).where(_.venuename matches p1).toString() must_== """db.venues.find({ "venuename" : { "$regex" : "Star.*" , "$options" : ""}})"""
    val p2 = Pattern.compile("Star.*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE)
    Q(ThriftVenue).where(_.venuename matches p2).toString() must_== """db.venues.find({ "venuename" : { "$regex" : "Star.*" , "$options" : "im"}})"""
    Q(ThriftVenue).where(_.venuename matches p2).and(_.venuename nin List("a", "b")).toString() must_== """db.venues.find({ "venuename" : { "$nin" : [ "a" , "b"] , "$regex" : "Star.*" , "$options" : "im"}})"""

    // all, in, size, contains, at
    Q(ThriftVenue).where(_.tags all List("db", "ka"))  .toString() must_== """db.venues.find({ "tags" : { "$all" : [ "db" , "ka"]}})"""
    Q(ThriftVenue).where(_.tags in  List("db", "ka"))  .toString() must_== """db.venues.find({ "tags" : { "$in" : [ "db" , "ka"]}})"""
    Q(ThriftVenue).where(_.tags nin List("db", "ka"))  .toString() must_== """db.venues.find({ "tags" : { "$nin" : [ "db" , "ka"]}})"""
    Q(ThriftVenue).where(_.tags neqs List("db", "ka")) .toString() must_== """db.venues.find({ "tags" : { "$ne" : [ "db" , "ka"]}})"""
    Q(ThriftVenue).where(_.tags size 3)                .toString() must_== """db.venues.find({ "tags" : { "$size" : 3}})"""
    Q(ThriftVenue).where(_.tags contains "karaoke")    .toString() must_== """db.venues.find({ "tags" : "karaoke"})"""
    Q(ThriftVenue).where(_.tags notcontains "karaoke") .toString() must_== """db.venues.find({ "tags" : { "$ne" : "karaoke"}})"""
    Q(ThriftVenue).where(_.popularity contains 3)      .toString() must_== """db.venues.find({ "popularity" : 3})"""
    Q(ThriftVenue).where(_.popularity at 0 eqs 3)      .toString() must_== """db.venues.find({ "popularity.0" : 3})"""
    Q(ThriftVenue).where(_.categories at 0 eqs oid)    .toString() must_== """db.venues.find({ "categories.0" : ObjectId("%s")})""".format(oid.toString)
    Q(ThriftVenue).where(_.tags at 0 startsWith "kara").toString() must_== """db.venues.find({ "tags.0" : { "$regex" : "^\\Qkara\\E" , "$options" : ""}})"""
    // alternative syntax
    Q(ThriftVenue).where(_.tags idx 0 startsWith "kara").toString() must_== """db.venues.find({ "tags.0" : { "$regex" : "^\\Qkara\\E" , "$options" : ""}})"""

    // maps
    Q(ThriftTip).where(_.counts at "foo" eqs 3).toString() must_== """db.tips.find({ "counts.foo" : 3})"""
    Q(ThriftVenue).where(_.lastCheckins.at("123").sub.field(_.id) eqs oid).toString must_== """db.venues.find({ "lastCheckins.123._id" : ObjectId("%s")})""".format(oid.toString)

    /* TODO(rogue-latlng)
    // near
    Q(ThriftVenue).where(_.geolatlng near (39.0, -74.0, Degrees(0.2)))    .toString() must_== """db.venues.find({ "latlng" : { "$near" : [ 39.0 , -74.0 , 0.2]}})"""
    Q(ThriftVenue).where(_.geolatlng withinCircle(1.0, 2.0, Degrees(0.3))).toString() must_== """db.venues.find({ "latlng" : { "$within" : { "$center" : [ [ 1.0 , 2.0] , 0.3]}}})"""
    Q(ThriftVenue).where(_.geolatlng withinBox(1.0, 2.0, 3.0, 4.0))       .toString() must_== """db.venues.find({ "latlng" : { "$within" : { "$box" : [ [ 1.0 , 2.0] , [ 3.0 , 4.0]]}}})"""
    Q(ThriftVenue).where(_.geolatlng eqs (45.0, 50.0))                    .toString() must_== """db.venues.find({ "latlng" : [ 45.0 , 50.0]})"""
    Q(ThriftVenue).where(_.geolatlng neqs (31.0, 23.0))                   .toString() must_== """db.venues.find({ "latlng" : { "$ne" : [ 31.0 , 23.0]}})"""
    Q(ThriftVenue).where(_.geolatlng eqs List(45.0, 50.0))             .toString() must_== """db.venues.find({ "latlng" : [ 45.0 , 50.0]})"""
    Q(ThriftVenue).where(_.geolatlng neqs List(31.0, 23.0))            .toString() must_== """db.venues.find({ "latlng" : { "$ne" : [ 31.0 , 23.0]}})"""
    */

    // ObjectId before, after, between
    Q(ThriftVenue).where(_.id before d2)       .toString() must_== """db.venues.find({ "_id" : { "$lt" : ObjectId("%s")}})""".format(oid2.toString)
    Q(ThriftVenue).where(_.id after d1)        .toString() must_== """db.venues.find({ "_id" : { "$gt" : ObjectId("%s")}})""".format(oid1.toString)
    Q(ThriftVenue).where(_.id between (d1, d2)).toString() must_== """db.venues.find({ "_id" : { "$gt" : ObjectId("%s") , "$lt" : ObjectId("%s")}})""".format(oid1.toString, oid2.toString)
    Q(ThriftVenue).where(_.id between Tuple2(d1, d2)).toString() must_== """db.venues.find({ "_id" : { "$gt" : ObjectId("%s") , "$lt" : ObjectId("%s")}})""".format(oid1.toString, oid2.toString)

    // DateTime before, after, between
    Q(ThriftVenue).where(_.last_updated before d2)       .toString() must_== """db.venues.find({ "last_updated" : { "$lt" : { "$date" : "2010-05-02T00:00:00.000Z"}}})"""
    Q(ThriftVenue).where(_.last_updated after d1)        .toString() must_== """db.venues.find({ "last_updated" : { "$gt" : { "$date" : "2010-05-01T00:00:00.000Z"}}})"""
    Q(ThriftVenue).where(_.last_updated between (d1, d2)).toString() must_== """db.venues.find({ "last_updated" : { "$gte" : { "$date" : "2010-05-01T00:00:00.000Z"} , "$lte" : { "$date" : "2010-05-02T00:00:00.000Z"}}})"""
    Q(ThriftVenue).where(_.last_updated between Tuple2(d1, d2)).toString() must_== """db.venues.find({ "last_updated" : { "$gte" : { "$date" : "2010-05-01T00:00:00.000Z"} , "$lte" : { "$date" : "2010-05-02T00:00:00.000Z"}}})"""
    Q(ThriftVenue).where(_.last_updated eqs d1)          .toString() must_== """db.venues.find({ "last_updated" : { "$date" : "2010-05-01T00:00:00.000Z"}})"""

    /* TODO(rogue-sub-on-listfield)
    // sub queries on listfield
    Q(ThriftComment).where(_.comments.unsafeField[Int]("z") contains 123).toString() must_== """db.comments.find({ "comments.z" : 123})"""
    Q(ThriftComment).where(_.comments.unsafeField[String]("comment") contains "hi").toString() must_== """db.comments.find({ "comments.comment" : "hi"})"""

    // BsonRecordField subfield queries
    Q(ThriftVenue).where(_.claims.sub.field(_.status) contains ThriftClaimStatus.approved).toString() must_== """db.venues.find({ "claims.status" : "Approved"})"""
    Q(ThriftVenue).where(_.lastClaim.sub.field(_.userid) eqs 123)              .toString()      must_== """db.venues.find({ "lastClaim.uid" : 123})"""
    Q(ThriftVenue).where(_.claims.sub.field(_.source.sub.field(_.name)) contains "twitter").toString() must_== """db.venues.find({ "claims.source.name" : "twitter"})"""
    */

    // Enumeration list
    /* TODO(rogue-named-enums)
    Q(ThriftOAuthConsumer).where(_.privileges contains ThriftConsumerPrivilege.awardBadges).toString() must_== """db.oauthconsumers.find({ "privileges" : "Award badges"})"""
    */

    /* TODO(rogue-sub-on-listfield)
    Q(ThriftOAuthConsumer).where(_.privileges at 0 eqs ThriftConsumerPrivilege.awardBadges).toString() must_== """db.oauthconsumers.find({ "privileges.0" : "Award badges"})"""
    */

    // Field type
    Q(ThriftVenue).where(_.legacyid hastype MongoType.String).toString() must_== """db.venues.find({ "legid" : { "$type" : 2}})"""

    // Modulus
    Q(ThriftVenue).where(_.legacyid mod (5, 1)).toString() must_== """db.venues.find({ "legid" : { "$mod" : [ 5 , 1]}})"""

    // compound queries
    Q(ThriftVenue).where(_.mayor eqs 1).and(_.tags contains "karaoke").toString() must_== """db.venues.find({ "mayor" : 1 , "tags" : "karaoke"})"""
    Q(ThriftVenue).where(_.mayor eqs 1).and(_.mayor_count eqs 5)      .toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : 5})"""
    Q(ThriftVenue).where(_.mayor eqs 1).and(_.mayor_count lt 5)       .toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : { "$lt" : 5}})"""
    Q(ThriftVenue).where(_.mayor eqs 1).and(_.mayor_count gt 3).and(_.mayor_count lt 5).toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : { "$lt" : 5 , "$gt" : 3}})"""

    // queries with no clauses
    Q(ThriftVenue).toString() must_== "db.venues.find({ })"
    Q(ThriftVenue).orderDesc(_.id).toString() must_== """db.venues.find({ }).sort({ "_id" : -1})"""

    // ordered queries
    Q(ThriftVenue).where(_.mayor eqs 1).orderAsc(_.legacyid).toString() must_== """db.venues.find({ "mayor" : 1}).sort({ "legid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).orderDesc(_.legacyid).andAsc(_.userid).toString() must_== """db.venues.find({ "mayor" : 1}).sort({ "legid" : -1 , "userid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).orderNaturalAsc.toString() must_== """db.venues.find({ "mayor" : 1}).sort({ "$natural" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).orderNaturalDesc.toString() must_== """db.venues.find({ "mayor" : 1}).sort({ "$natural" : -1})"""

    // select queries
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid, _.userid).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid, _.userid, _.mayor).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid, _.userid, _.mayor, _.mayor_count).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1 , "closed" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1 , "closed" : 1 , "tags" : 1})"""

    // select case queries
    Q(ThriftVenue).where(_.mayor eqs 1).selectCase(_.legacyid, V1).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).selectCase(_.legacyid, _.userid, V2).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).selectCase(_.legacyid, _.userid, _.mayor, V3).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, V4).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, V5).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1 , "closed" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags, V6).toString() must_== """db.venues.find({ "mayor" : 1}, { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1 , "closed" : 1 , "tags" : 1})"""

    // select subfields
    Q(ThriftTip).where(_.legacyid eqs 1).select(_.counts at "foo").toString() must_== """db.tips.find({ "legid" : 1}, { "counts.foo" : 1})"""
    /* TODO(rogue-latlng)
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.geolatlng.unsafeField[Double]("lat")).toString() must_== """db.venues.find({ "legid" : 1}, { "latlng.lat" : 1})"""
    */
    /* TODO(rogue-named-enums)
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.lastClaim.sub.select(_.status)).toString() must_== """db.venues.find({ "legid" : 1}, { "lastClaim.status" : 1})"""
    */
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.claims.sub.select(_.userid)).toString() must_== """db.venues.find({ "legid" : 1}, { "claims.uid" : 1})"""

    // select slice
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.tags).toString() must_== """db.venues.find({ "legid" : 1}, { "tags" : 1})"""
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.tags.slice(4)).toString() must_== """db.venues.find({ "legid" : 1}, { "tags" : { "$slice" : 4}})"""
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.tags.slice(4, 7)).toString() must_== """db.venues.find({ "legid" : 1}, { "tags" : { "$slice" : [ 4 , 7]}})"""

    Q(ThriftComment).select(_.comments.unsafeField[Long]("userid")).toString() must_== """db.comments.find({ }, { "comments.userid" : 1})"""

    // out of order and doesn't screw up earlier params
    Q(ThriftVenue).limit(10).where(_.mayor eqs 1).toString() must_== """db.venues.find({ "mayor" : 1}).limit(10)"""
    Q(ThriftVenue).orderDesc(_.id).and(_.mayor eqs 1).toString() must_== """db.venues.find({ "mayor" : 1}).sort({ "_id" : -1})"""
    Q(ThriftVenue).orderDesc(_.id).skip(3).and(_.mayor eqs 1).toString() must_== """db.venues.find({ "mayor" : 1}).sort({ "_id" : -1}).skip(3)"""

    // Scan should be the same as and/where
    Q(ThriftVenue).where(_.mayor eqs 1).scan(_.tags contains "karaoke").toString() must_== """db.venues.find({ "mayor" : 1 , "tags" : "karaoke"})"""
    Q(ThriftVenue).scan(_.mayor eqs 1).and(_.mayor_count eqs 5)        .toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : 5})"""
    Q(ThriftVenue).scan(_.mayor eqs 1).scan(_.mayor_count lt 5)        .toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : { "$lt" : 5}})"""

    // limit, limitOpt, skip, skipOpt
    Q(ThriftVenue).where(_.mayor eqs 1).limit(10)         .toString() must_== """db.venues.find({ "mayor" : 1}).limit(10)"""
    Q(ThriftVenue).where(_.mayor eqs 1).limitOpt(Some(10)).toString() must_== """db.venues.find({ "mayor" : 1}).limit(10)"""
    Q(ThriftVenue).where(_.mayor eqs 1).limitOpt(None)    .toString() must_== """db.venues.find({ "mayor" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).skip(10)          .toString() must_== """db.venues.find({ "mayor" : 1}).skip(10)"""
    Q(ThriftVenue).where(_.mayor eqs 1).skipOpt(Some(10)) .toString() must_== """db.venues.find({ "mayor" : 1}).skip(10)"""
    Q(ThriftVenue).where(_.mayor eqs 1).skipOpt(None)     .toString() must_== """db.venues.find({ "mayor" : 1})"""

    // raw query clauses
    Q(ThriftVenue).where(_.mayor eqs 1).raw(_.add("$where", "this.a > 3")).toString() must_== """db.venues.find({ "mayor" : 1 , "$where" : "this.a > 3"})"""

    // $not tests
    Q(ThriftVenue).scan(_.mayor eqs 1).scan(_.mayor_count not (_ lt 5)).toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : { "$not" : { "$lt" : 5}}})"""
    Q(ThriftVenue).scan(_.mayor eqs 1).scan(_.mayor_count not (_ lt 5)).and(_.mayor_count not(_ gt 6)).toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : { "$not" : { "$gt" : 6 , "$lt" : 5}}})"""
    Q(ThriftVenue).scan(_.mayor eqs 1).scan(_.mayor_count not (_ lt 5)).and(_.mayor_count gt 3).toString() must_== """db.venues.find({ "mayor" : 1 , "mayor_count" : { "$gt" : 3 , "$not" : { "$lt" : 5}}})"""
    Q(ThriftVenue).scan(_.id not (_ before d1)).toString() must_== """db.venues.find({ "_id" : { "$not" : { "$lt" : ObjectId("%s")}}})""".format(oid1.toString)
    Q(ThriftVenue).scan(_.last_updated not (_ between (d1, d2))).toString() must_== """db.venues.find({ "last_updated" : { "$not" : { "$gte" : { "$date" : "2010-05-01T00:00:00.000Z"} , "$lte" : { "$date" : "2010-05-02T00:00:00.000Z"}}}})"""
    Q(ThriftVenue).scan(_.tags not (_ in List("a", "b"))).toString()  must_==  """db.venues.find({ "tags" : { "$not" : { "$in" : [ "a" , "b"]}}})"""
    Q(ThriftVenue).scan(_.tags not (_ size 0)).toString()  must_==  """db.venues.find({ "tags" : { "$not" : { "$size" : 0}}})"""
    Q(ThriftVenue).scan(_.popularity at 0 not (_ lt 0)).toString() must_==  """db.venues.find({ "popularity.0" : { "$not" : { "$lt" : 0}}})"""
  }

  @Test
  def testModifyQueryShouldProduceACorrectJSONQueryString {
    val d1 = new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)

    val query = """db.venues.update({ "legid" : 1}, """
    val suffix = ", false, false)"
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.venuename setTo "fshq").toString() must_== query + """{ "$set" : { "venuename" : "fshq"}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count setTo 3)   .toString() must_== query + """{ "$set" : { "mayor_count" : 3}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count unset)     .toString() must_== query + """{ "$unset" : { "mayor_count" : 1}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count setTo Some(3)).toString() must_== query + """{ "$set" : { "mayor_count" : 3}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count setTo None)   .toString() must_== query + """{ "$unset" : { "mayor_count" : 1}}""" + suffix

    // Numeric
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count inc 3).toString() must_== query + """{ "$inc" : { "mayor_count" : 3}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count mul 3).toString() must_== query + """{ "$mul" : { "mayor_count" : 3}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count min 3).toString() must_== query + """{ "$min" : { "mayor_count" : 3}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count max 3).toString() must_== query + """{ "$max" : { "mayor_count" : 3}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.last_updated min d1).toString() must_== query + """{ "$min" : { "last_updated" : { "$date" : "2010-05-01T00:00:00.000Z"}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.last_updated max d1).toString() must_== query + """{ "$max" : { "last_updated" : { "$date" : "2010-05-01T00:00:00.000Z"}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.last_updated currentDate).toString() must_== query + """{ "$currentDate" : { "last_updated" : true}}""" + suffix

    // Enumeration
    /* TODO(rogue-named-enums)
    val query2 = """db.venueclaims.update({ "uid" : 1}, """
    Q(ThriftVenueClaim).where(_.userid eqs 1).modify(_.status setTo ThriftClaimStatus.approved).toString() must_== query2 + """{ "$set" : { "status" : "Approved"}}""" + suffix
    */

    // Calendar
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.last_updated setTo d1).toString() must_== query + """{ "$set" : { "last_updated" : { "$date" : "2010-05-01T00:00:00.000Z"}}}""" + suffix
    /* TODO(rogue-date-support): no support for java.util.Date
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.last_updated setTo d1.toDate).toString() must_== query + """{ "$set" : { "last_updated" : { "$date" : "2010-05-01T00:00:00.000Z"}}}""" + suffix
    */

    // LatLong
    val ll = List(37.4, -73.9)
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.geolatlng setTo ll).toString() must_== query + """{ "$set" : { "latlng" : [ 37.4 , -73.9]}}""" + suffix

    // Lists
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity setTo List(5))      .toString() must_== query + """{ "$set" : { "popularity" : [ 5]}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity push 5)             .toString() must_== query + """{ "$push" : { "popularity" : 5}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.tags pushAll List("a", "b"))   .toString() must_== query + """{ "$pushAll" : { "tags" : [ "a" , "b"]}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.tags addToSet "a")             .toString() must_== query + """{ "$addToSet" : { "tags" : "a"}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity addToSet List(1, 2)).toString() must_== query + """{ "$addToSet" : { "popularity" : { "$each" : [ 1 , 2]}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.tags popFirst)                 .toString() must_== query + """{ "$pop" : { "tags" : -1}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.tags popLast)                  .toString() must_== query + """{ "$pop" : { "tags" : 1}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.tags pull "a")                 .toString() must_== query + """{ "$pull" : { "tags" : "a"}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity pullAll List(2, 3)) .toString() must_== query + """{ "$pullAll" : { "popularity" : [ 2 , 3]}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity at 0 inc 1)         .toString() must_== query + """{ "$inc" : { "popularity.0" : 1}}""" + suffix
    // alternative syntax
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity idx 0 inc 1)        .toString() must_== query + """{ "$inc" : { "popularity.0" : 1}}""" + suffix

    // Enumeration list
    /* TODO(rogue-named-enums)
    Q(ThriftOAuthConsumer).modify(_.privileges addToSet ThriftConsumerPrivilege.awardBadges).toString() must_== """db.oauthconsumers.update({ }, { "$addToSet" : { "privileges" : "Award badges"}}""" + suffix
    */

    // BsonRecordField and BsonRecordListField with nested Enumeration
    val src = ThriftSourceBson.newBuilder.name("").url("").result
    val claims = List(ThriftVenueClaimBson.newBuilder.userid(1).status(ThriftClaimStatus.approved).source(src).result)
    /*  TODO(rogue-named-enums)
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.claims setTo claims).toString() must_== query + """{ "$set" : { "claims" : [ { "uid" : 1 , "status" : "Approved" , "source" : { "name" : "" , "url" : ""}}]}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.lastClaim setTo claims.head).toString() must_== query + """{ "$set" : { "last_claim" : { "uid" : 1 , "status" : "Approved" , "source" : { "name" : "" , "url" : ""}}}}""" + suffix
    */

    // Map
    val m = Map("foo" -> 1)
    val query3 = """db.tips.update({ "legid" : 1}, """
    Q(ThriftTip).where(_.legacyid eqs 1).modify(_.counts setTo m)         .toString() must_== query3 + """{ "$set" : { "counts" : { "foo" : 1}}}""" + suffix
    Q(ThriftTip).where(_.legacyid eqs 1).modify(_.counts at "foo" setTo 3).toString() must_== query3 + """{ "$set" : { "counts.foo" : 3}}""" + suffix
    Q(ThriftTip).where(_.legacyid eqs 1).modify(_.counts at "foo" inc 5)  .toString() must_== query3 + """{ "$inc" : { "counts.foo" : 5}}""" + suffix
    Q(ThriftTip).where(_.legacyid eqs 1).modify(_.counts at "foo" unset)  .toString() must_== query3 + """{ "$unset" : { "counts.foo" : 1}}""" + suffix
    Q(ThriftTip).where(_.legacyid eqs 1).modify(_.counts setTo Map("foo" -> 3, "bar" -> 5)).toString() must_== query3 + """{ "$set" : { "counts" : { "foo" : 3 , "bar" : 5}}}""" + suffix

    // Multiple updates
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.venuename setTo "fshq").and(_.mayor_count setTo 3).toString() must_== query + """{ "$set" : { "mayor_count" : 3 , "venuename" : "fshq"}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.venuename setTo "fshq").and(_.mayor_count inc 1)  .toString() must_== query + """{ "$set" : { "venuename" : "fshq"} , "$inc" : { "mayor_count" : 1}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.venuename setTo "fshq").and(_.mayor_count setTo 3).and(_.mayor_count inc 1).toString() must_== query + """{ "$set" : { "mayor_count" : 3 , "venuename" : "fshq"} , "$inc" : { "mayor_count" : 1}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.popularity addToSet 3).and(_.tags addToSet List("a", "b")).toString() must_== query + """{ "$addToSet" : { "tags" : { "$each" : [ "a" , "b"]} , "popularity" : 3}}""" + suffix

    // Noop query
    Q(ThriftVenue).where(_.legacyid eqs 1).noop().toString() must_== query + "{ }" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).noop().modify(_.venuename setTo "fshq").toString() must_== query + """{ "$set" : { "venuename" : "fshq"}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).noop().and(_.venuename setTo "fshq")   .toString() must_== query + """{ "$set" : { "venuename" : "fshq"}}""" + suffix

    // $bit
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count bitAnd 3).toString() must_== query + """{ "$bit" : { "mayor_count" : { "and" : 3}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count bitOr 3).toString() must_== query + """{ "$bit" : { "mayor_count" : { "or" : 3}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.mayor_count bitXor 3).toString() must_== query + """{ "$bit" : { "mayor_count" : { "xor" : 3}}}""" + suffix

    // $rename
    Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.venuename rename "vn").toString() must_== query + """{ "$rename" : { "venuename" : "vn"}}""" + suffix

    // pullWhere
    /*
    tags is list<string>
    popularity is list<i32>
    categories is list<ObjectId>
    claims is list<ThriftVenueClaimBson>
    */
    Q(ThriftVenue).where(_.legacyid eqs 1)
         .modify(_.tags pullWhere(_ startsWith "prefix"))
         .toString() must_== query + """{ "$pull" : { "tags" : { "$regex" : "^\\Qprefix\\E" , "$options" : ""}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1)
         .modify(_.popularity pullWhere(_ gt 2))
         .toString() must_== query + """{ "$pull" : { "popularity" : { "$gt" : 2}}}""" + suffix
    Q(ThriftVenue).where(_.legacyid eqs 1)
         .modify(_.popularity pullWhere(_ gt 2, _ lt 5))
         .toString() must_== query + """{ "$pull" : { "popularity" : { "$gt" : 2 , "$lt" : 5}}}""" + suffix
    /* TODO(rogue-pullObjectWhere) and TODO(rogue-named-enums)
    Q(ThriftVenue).where(_.legacyid eqs 1)
         .modify(_.claims pullObjectWhere(_.status eqs ThriftClaimStatus.approved,
                                          _.userid eqs 2097))
         .toString() must_== query + """{ "$pull" : { "claims" : { "uid" : 2097 , "status" : "Approved"}}}""" + suffix
    */
  }

  @Test
  def testProduceACorrectSignatureString {
    val d1 = new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)
    val d2 = new DateTime(2010, 5, 2, 0, 0, 0, 0, DateTimeZone.UTC)
    val oid = new ObjectId

    // basic ops
    Q(ThriftVenue).where(_.mayor eqs 1)              .signature() must_== """db.venues.find({ "mayor" : 0})"""
    Q(ThriftVenue).where(_.venuename eqs "Starbucks").signature() must_== """db.venues.find({ "venuename" : 0})"""
    Q(ThriftVenue).where(_.closed eqs true)          .signature() must_== """db.venues.find({ "closed" : 0})"""
    Q(ThriftVenue).where(_.id eqs VenueId(oid))  .signature() must_== """db.venues.find({ "_id" : 0})"""
    Q(ThriftVenueClaim).where(_.status eqs ThriftClaimStatus.approved).signature() must_== """db.venueclaims.find({ "status" : 0})"""
    Q(ThriftVenue).where(_.mayor_count gte 5).signature() must_== """db.venues.find({ "mayor_count" : { "$gte" : 0}})"""
    Q(ThriftVenueClaim).where(_.status neqs ThriftClaimStatus.approved).signature() must_== """db.venueclaims.find({ "status" : { "$ne" : 0}})"""
    Q(ThriftVenue).where(_.legacyid in List(123L, 456L)).signature() must_== """db.venues.find({ "legid" : { "$in" : 0}})"""
    Q(ThriftVenue).where(_.id exists true).signature() must_== """db.venues.find({ "_id" : { "$exists" : 0}})"""
    Q(ThriftVenue).where(_.venuename startsWith "Starbucks").signature() must_== """db.venues.find({ "venuename" : { "$regex" : 0 , "$options" : 0}})"""

    // list
    Q(ThriftVenue).where(_.tags all List("db", "ka"))   .signature() must_== """db.venues.find({ "tags" : { "$all" : 0}})"""
    Q(ThriftVenue).where(_.tags in  List("db", "ka"))   .signature() must_== """db.venues.find({ "tags" : { "$in" : 0}})"""
    Q(ThriftVenue).where(_.tags size 3)                 .signature() must_== """db.venues.find({ "tags" : { "$size" : 0}})"""
    Q(ThriftVenue).where(_.tags contains "karaoke")     .signature() must_== """db.venues.find({ "tags" : 0})"""
    Q(ThriftVenue).where(_.popularity contains 3)       .signature() must_== """db.venues.find({ "popularity" : 0})"""
    Q(ThriftVenue).where(_.popularity at 0 eqs 3)       .signature() must_== """db.venues.find({ "popularity.0" : 0})"""
    Q(ThriftVenue).where(_.categories at 0 eqs oid)     .signature() must_== """db.venues.find({ "categories.0" : 0})"""
    Q(ThriftVenue).where(_.tags at 0 startsWith "kara") .signature() must_== """db.venues.find({ "tags.0" : { "$regex" : 0 , "$options" : 0}})"""
    Q(ThriftVenue).where(_.tags idx 0 startsWith "kara").signature() must_== """db.venues.find({ "tags.0" : { "$regex" : 0 , "$options" : 0}})"""

    // map
    Q(ThriftTip).where(_.counts at "foo" eqs 3).signature() must_== """db.tips.find({ "counts.foo" : 0})"""

    /* TODO(rogue-latlng)
    // near
    Q(ThriftVenue).where(_.geolatlng near (39.0, -74.0, Degrees(0.2)))    .signature() must_== """db.venues.find({ "latlng" : { "$near" : 0}})"""
    Q(ThriftVenue).where(_.geolatlng withinCircle(1.0, 2.0, Degrees(0.3))).signature() must_== """db.venues.find({ "latlng" : { "$within" : { "$center" : 0}}})"""
    Q(ThriftVenue).where(_.geolatlng withinBox(1.0, 2.0, 3.0, 4.0))       .signature() must_== """db.venues.find({ "latlng" : { "$within" : { "$box" : 0}}})"""
    Q(ThriftVenue).where(_.geolatlng eqs (45.0, 50.0)).signature() must_== """db.venues.find({ "latlng" : 0})"""
    */

    // id, date range
    Q(ThriftVenue).where(_.id before d2).signature()          must_== """db.venues.find({ "_id" : { "$lt" : 0}})"""
    Q(ThriftVenue).where(_.last_updated before d2).signature() must_== """db.venues.find({ "last_updated" : { "$lt" : 0}})"""

    // Case class list field
    /* TODO(rogue-sub-on-listfield)
    Q(ThriftComment).where(_.comments.unsafeField[Int]("z") contains 123)          .signature() must_== """db.comments.find({ "comments.z" : 0})"""
    Q(ThriftComment).where(_.comments.unsafeField[String]("comment") contains "hi").signature() must_== """db.comments.find({ "comments.comment" : 0})"""
    */

    // Enumeration list
    Q(ThriftOAuthConsumer).where(_.privileges contains ThriftConsumerPrivilege.awardBadges).signature() must_== """db.oauthconsumers.find({ "privileges" : 0})"""
    Q(ThriftOAuthConsumer).where(_.privileges at 0 eqs ThriftConsumerPrivilege.awardBadges).signature() must_== """db.oauthconsumers.find({ "privileges.0" : 0})"""

    // Field type
    Q(ThriftVenue).where(_.legacyid hastype MongoType.String).signature() must_== """db.venues.find({ "legid" : { "$type" : 0}})"""

    // Modulus
    Q(ThriftVenue).where(_.legacyid mod (5, 1)).signature() must_== """db.venues.find({ "legid" : { "$mod" : 0}})"""

    // compound queries
    Q(ThriftVenue).where(_.mayor eqs 1).and(_.tags contains "karaoke").signature() must_== """db.venues.find({ "mayor" : 0 , "tags" : 0})"""
    Q(ThriftVenue).where(_.mayor eqs 1).and(_.mayor_count gt 3).and(_.mayor_count lt 5).signature() must_== """db.venues.find({ "mayor" : 0 , "mayor_count" : { "$lt" : 0 , "$gt" : 0}})"""

    // queries with no clauses
    Q(ThriftVenue).signature() must_== "db.venues.find({ })"
    Q(ThriftVenue).orderDesc(_.id)         .signature() must_== """db.venues.find({ }).sort({ "_id" : -1})"""

    // ordered queries
    Q(ThriftVenue).where(_.mayor eqs 1).orderAsc(_.legacyid).signature() must_== """db.venues.find({ "mayor" : 0}).sort({ "legid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 1).orderDesc(_.legacyid).andAsc(_.userid).signature() must_== """db.venues.find({ "mayor" : 0}).sort({ "legid" : -1 , "userid" : 1})"""

    // select queries
    Q(ThriftVenue).where(_.mayor eqs 1).select(_.legacyid).signature() must_== """db.venues.find({ "mayor" : 0})"""

    // Scan should be the same as and/where
    Q(ThriftVenue).where(_.mayor eqs 1).scan(_.tags contains "karaoke").signature() must_== """db.venues.find({ "mayor" : 0 , "tags" : 0})"""
  }

  @Test
  def testFindAndModifyQueryShouldProduceACorrectJSONQueryString {
    Q(ThriftVenue).where(_.legacyid eqs 1).findAndModify(_.venuename setTo "fshq").toString().must_==(
      """db.venues.findAndModify({ query: { "legid" : 1}, update: { "$set" : { "venuename" : "fshq"}}, new: false, upsert: false })""")
    Q(ThriftVenue).where(_.legacyid eqs 1).orderAsc(_.popularity).findAndModify(_.venuename setTo "fshq").toString().must_==(
      """db.venues.findAndModify({ query: { "legid" : 1}, sort: { "popularity" : 1}, update: { "$set" : { "venuename" : "fshq"}}, new: false, upsert: false })""")
    Q(ThriftVenue).where(_.legacyid eqs 1).select(_.mayor, _.closed).findAndModify(_.venuename setTo "fshq").toString().must_==(
      """db.venues.findAndModify({ query: { "legid" : 1}, update: { "$set" : { "venuename" : "fshq"}}, new: false, fields: { "mayor" : 1 , "closed" : 1}, upsert: false })""")
  }

  @Test
  def testOrQueryShouldProduceACorrectJSONQueryString {
    // Simple $or
    Q(ThriftVenue).or(
        _.where(_.legacyid eqs 1),
        _.where(_.mayor eqs 2))
      .toString() must_== """db.venues.find({ "$or" : [ { "legid" : 1} , { "mayor" : 2}]})"""

    // Compound $or
    Q(ThriftVenue).where(_.tags size 0)
         .or(
           _.where(_.legacyid eqs 1),
           _.where(_.mayor eqs 2))
      .toString() must_== """db.venues.find({ "tags" : { "$size" : 0} , "$or" : [ { "legid" : 1} , { "mayor" : 2}]})"""

    // $or with additional "and" clauses
    Q(ThriftVenue).where(_.tags size 0)
         .or(
           _.where(_.legacyid eqs 1).and(_.closed eqs true),
           _.where(_.mayor eqs 2))
      .toString() must_== """db.venues.find({ "tags" : { "$size" : 0} , "$or" : [ { "legid" : 1 , "closed" : true} , { "mayor" : 2}]})"""

    // Nested $or
    Q(ThriftVenue).or(
        _.where(_.legacyid eqs 1)
         .or(
            _.where(_.closed eqs true),
            _.where(_.closed exists false)),
        _.where(_.mayor eqs 2))
      .toString() must_== """db.venues.find({ "$or" : [ { "legid" : 1 , "$or" : [ { "closed" : true} , { "closed" : { "$exists" : false}}]} , { "mayor" : 2}]})"""

    // $or with modify
    Q(ThriftVenue).or(
        _.where(_.legacyid eqs 1),
        _.where(_.mayor eqs 2))
      .modify(_.userid setTo 1).toString() must_== """db.venues.update({ "$or" : [ { "legid" : 1} , { "mayor" : 2}]}, { "$set" : { "userid" : 1}}, false, false)"""

    // OrQuery syntax
    val q1 = Q(ThriftVenue).where(_.legacyid eqs 1)
    val q2 = Q(ThriftVenue).where(_.legacyid eqs 2)
    /* TODO(rogue-missing-OrQuery)
    OrQuery(q1, q2).toString() must_==
        """db.venues.find({ "$or" : [ { "legid" : 1} , { "legid" : 2}]})"""
    OrQuery(q1, q2).and(_.mayor eqs 0).toString() must_==
        """db.venues.find({ "mayor" : 0 , "$or" : [ { "legid" : 1} , { "legid" : 2}]})"""
    OrQuery(q1, q2.or(_.where(_.closed eqs true), _.where(_.closed exists false))).toString() must_==
        """db.venues.find({ "$or" : [ { "legid" : 1} , { "legid" : 2 , "$or" : [ { "closed" : true} , { "closed" : { "$exists" : false}}]}]})"""
    */
  }

  @Test
  def testHints {
    /* TODO(rogue-index-support)
    Q(ThriftVenue).where(_.legacyid eqs 1).hint(Q(ThriftVenue).idIdx).toString()        must_== """db.venues.find({ "legid" : 1}).hint({ "_id" : 1})"""
    Q(ThriftVenue).where(_.legacyid eqs 1).hint(Q(ThriftVenue).legIdx).toString()       must_== """db.venues.find({ "legid" : 1}).hint({ "legid" : -1})"""
    Q(ThriftVenue).where(_.legacyid eqs 1).hint(Q(ThriftVenue).geoIdx).toString()       must_== """db.venues.find({ "legid" : 1}).hint({ "latlng" : "2d"})"""
    Q(ThriftVenue).where(_.legacyid eqs 1).hint(Q(ThriftVenue).geoCustomIdx).toString() must_== """db.venues.find({ "legid" : 1}).hint({ "latlng" : "custom" , "tags" : 1})"""
    */
  }

  @Test
  def testDollarSelector {
    /* TODO(rogue-sub-on-listfield) and TODO(rogue-named-enums)
    Q(ThriftVenue).where(_.legacyid eqs 1)
         .and(_.claims.sub.field(_.userid) contains 2)
         .modify(_.claims.$.sub.field(_.status) setTo ThriftClaimStatus.approved)
         .toString() must_== """db.venues.update({ "legid" : 1 , "claims.uid" : 2}, { "$set" : { "claims.$.status" : "Approved"}}, false, false)"""
    */

    Q(ThriftVenue).where(_.legacyid eqs 1)
         .and(_.tags contains "sometag")
         .modify(_.tags.$ setTo "othertag")
         .toString() must_== """db.venues.update({ "legid" : 1 , "tags" : "sometag"}, { "$set" : { "tags.$" : "othertag"}}, false, false)"""
  }

  @Test
  def testWhereOpt {
    val someId = Some(1L)
    val noId: Option[Long] = None
    val someList = Some(List(1L, 2L))
    val noList: Option[List[Long]] = None

    // whereOpt
    Q(ThriftVenue).whereOpt(someId)(_.legacyid eqs _).toString() must_== """db.venues.find({ "legid" : 1})"""
    Q(ThriftVenue).whereOpt(noId)(_.legacyid eqs _).toString() must_== """db.venues.find({ })"""
    Q(ThriftVenue).whereOpt(someId)(_.legacyid eqs _).and(_.mayor eqs 2).toString() must_== """db.venues.find({ "legid" : 1 , "mayor" : 2})"""
    Q(ThriftVenue).whereOpt(noId)(_.legacyid eqs _).and(_.mayor eqs 2).toString() must_== """db.venues.find({ "mayor" : 2})"""

    // whereOpt: lists
    Q(ThriftVenue).whereOpt(someList)(_.legacyid in _).toString() must_== """db.venues.find({ "legid" : { "$in" : [ 1 , 2]}})"""
    Q(ThriftVenue).whereOpt(noList)(_.legacyid in _).toString() must_== """db.venues.find({ })"""

    // whereOpt: enum
    /* TODO(rogue-named-enums)
    val someEnum = Some(ThriftVenueStatus.open)
    val noEnum: Option[ThriftVenueStatus.type#Value] = None
    Q(ThriftVenue).whereOpt(someEnum)(_.status eqs _).toString() must_== """db.venues.find({ "status" : "Open"})"""
    Q(ThriftVenue).whereOpt(noEnum)(_.status eqs _).toString() must_== """db.venues.find({ })"""
    */

    // whereOpt: date
    val someDate = Some(new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC))
    val noDate: Option[DateTime] = None
    Q(ThriftVenue).whereOpt(someDate)(_.last_updated after _).toString() must_== """db.venues.find({ "last_updated" : { "$gt" : { "$date" : "2010-05-01T00:00:00.000Z"}}})"""
    Q(ThriftVenue).whereOpt(noDate)(_.last_updated after _).toString() must_== """db.venues.find({ })"""

    // andOpt
    Q(ThriftVenue).where(_.mayor eqs 2).andOpt(someId)(_.legacyid eqs _).toString() must_== """db.venues.find({ "mayor" : 2 , "legid" : 1})"""
    Q(ThriftVenue).where(_.mayor eqs 2).andOpt(noId)(_.legacyid eqs _).toString() must_== """db.venues.find({ "mayor" : 2})"""

    // scanOpt
    Q(ThriftVenue).scanOpt(someId)(_.legacyid eqs _).toString() must_== """db.venues.find({ "legid" : 1})"""
    Q(ThriftVenue).scanOpt(noId)(_.legacyid eqs _).toString() must_== """db.venues.find({ })"""
    Q(ThriftVenue).scanOpt(someId)(_.legacyid eqs _).and(_.mayor eqs 2).toString() must_== """db.venues.find({ "legid" : 1 , "mayor" : 2})"""
    Q(ThriftVenue).scanOpt(noId)(_.legacyid eqs _).and(_.mayor eqs 2).toString() must_== """db.venues.find({ "mayor" : 2})"""

    // iscanOpt
    Q(ThriftVenue).iscanOpt(someId)(_.legacyid eqs _).toString() must_== """db.venues.find({ "legid" : 1})"""
    Q(ThriftVenue).iscanOpt(noId)(_.legacyid eqs _).toString() must_== """db.venues.find({ })"""
    Q(ThriftVenue).iscanOpt(someId)(_.legacyid eqs _).and(_.mayor eqs 2).toString() must_== """db.venues.find({ "legid" : 1 , "mayor" : 2})"""
    Q(ThriftVenue).iscanOpt(noId)(_.legacyid eqs _).and(_.mayor eqs 2).toString() must_== """db.venues.find({ "mayor" : 2})"""

    // modify
    val q = Q(ThriftVenue).where(_.legacyid eqs 1)
    val prefix = """db.venues.update({ "legid" : 1}, """
    val suffix = ", false, false)"

    q.modifyOpt(someId)(_.legacyid setTo _).toString() must_== prefix + """{ "$set" : { "legid" : 1}}""" + suffix
    q.modifyOpt(noId)(_.legacyid setTo _).toString() must_== prefix + """{ }""" + suffix
    /* TODO(rogue-named-enums)
    q.modifyOpt(someEnum)(_.status setTo _).toString() must_== prefix + """{ "$set" : { "status" : "Open"}}""" + suffix
    q.modifyOpt(noEnum)(_.status setTo _).toString() must_== prefix + """{ }""" + suffix
    */
  }

  @Test
  def testShardKey {
    /* TODO(rogue-shards)
    Q(ThriftLike).where(_.checkin eqs 123).toString() must_== """db.likes.find({ "checkin" : 123})"""
    Q(ThriftLike).where(_.userid eqs 123).toString() must_== """db.likes.find({ "userid" : 123})"""
    Q(ThriftLike).where(_.userid eqs 123).allShards.toString() must_== """db.likes.find({ "userid" : 123})"""
    Q(ThriftLike).where(_.userid eqs 123).allShards.noop().toString() must_== """db.likes.update({ "userid" : 123}, { }, false, false)"""
    Q(ThriftLike).withShardKey(_.userid eqs 123).toString() must_== """db.likes.find({ "userid" : 123})"""
    Q(ThriftLike).withShardKey(_.userid in List(123, 456)).toString() must_== """db.likes.find({ "userid" : { "$in" : [ 123 , 456]}})"""
    Q(ThriftLike).withShardKey(_.userid eqs 123).and(_.checkin eqs 1).toString() must_== """db.likes.find({ "userid" : 123 , "checkin" : 1})"""
    Q(ThriftLike).where(_.checkin eqs 1).withShardKey(_.userid eqs 123).toString() must_== """db.likes.find({ "checkin" : 1 , "userid" : 123})"""
    */
  }


  @Test
  def testCommonSuperclassForPhantomTypes {
    def maybeLimit(legid: Long, limitOpt: Option[Int]) = {
      limitOpt match {
        case Some(limit) => Q(ThriftVenue).where(_.legacyid eqs legid).limit(limit)
        case None => Q(ThriftVenue).where(_.legacyid eqs legid)
      }
    }

    maybeLimit(1, None).toString() must_== """db.venues.find({ "legid" : 1})"""
    maybeLimit(1, Some(5)).toString() must_== """db.venues.find({ "legid" : 1}).limit(5)"""
  }

  @Test
  def testSetReadPreference: Unit = {
    type Q = Query[ThriftVenue, ThriftVenue, _]

    Q(ThriftVenue).where(_.mayor eqs 2).asInstanceOf[Q].readPreference must_== None
    Q(ThriftVenue).where(_.mayor eqs 2).setReadPreference(ReadPreference.secondary()).asInstanceOf[Q].readPreference must_== Some(ReadPreference.secondary())
    Q(ThriftVenue).where(_.mayor eqs 2).setReadPreference(ReadPreference.primary()).asInstanceOf[Q].readPreference must_== Some(ReadPreference.primary())
    Q(ThriftVenue).where(_.mayor eqs 2).setReadPreference(ReadPreference.secondary()).setReadPreference(ReadPreference.primary()).asInstanceOf[Q].readPreference must_== Some(ReadPreference.primary())
  }

  @Test
  def testQueryOptimizerDetectsEmptyQueries: Unit = {
    val optimizer = new QueryOptimizer

    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.mayor eqs 1)) must_== false
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.mayor in List())) must_== true
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.tags in List())) must_== true
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.tags all List())) must_== true
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.tags contains "karaoke").and(_.mayor in List())) must_== true
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.mayor in List()).and(_.tags contains "karaoke")) must_== true
    optimizer.isEmptyQuery(Q(ThriftComment).where(_.comments in List())) must_== true
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.mayor in List()).scan(_.mayor_count eqs 5)) must_== true
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.mayor eqs 1).modify(_.venuename setTo "fshq")) must_== false
    optimizer.isEmptyQuery(Q(ThriftVenue).where(_.mayor in List()).modify(_.venuename setTo "fshq")) must_== true
  }

  /* TODO(rogue-whenMigrated): creating a new Compiler object fails here, but should work in rogue package
   *                           Remove the @Ignore when migrated
   */
  @Ignore @Test
  def thingsThatShouldntCompile {
    val compiler = new CompilerForNegativeTests(List(
      """import com.foursquare.rogue._""",
      """import com.foursquare.roguev2._""",
      """import com.foursquare.roguev2.{Query = >Q}""",
      /* TODO(rogue-whenMigrated)
      """import com.foursquare.rogue.LiftRogue._""",
      */
      """import org.bson.types.ObjectId""",
      """import org.joda.time.DateTime"""))

    def check(code: String, expectedErrorREOpt: Option[String] = Some("")): Unit =
      compiler.check(code, expectedErrorREOpt)

    // For sanity
    // Q(ThriftVenue).where(_.legacyid eqs 3)
    check("""Q(ThriftVenue).where(_.legacyid eqs 3)""", None)

    // Basic operator and operand type matching
    check("""Q(ThriftVenue).where(_.legacyid eqs "hi")""")
    check("""Q(ThriftVenue).where(_.legacyid contains 3)""")
    check("""Q(ThriftVenue).where(_.tags contains 3)""")
    check("""Q(ThriftVenue).where(_.tags all List(3))""")
    check("""Q(ThriftVenue).where(_.geolatlng.unsafeField[String]("lat") eqs 3)""")
    check("""Q(ThriftVenue).where(_.closed eqs "false")""")
    check("""Q(ThriftVenue).where(_.tags < 3)""")
    check("""Q(ThriftVenue).where(_.tags size < 3)""")
    check("""Q(ThriftVenue).where(_.tags size "3")""")
    check("""Q(ThriftVenue).where(_.legacyid size 3)""")
    check("""Q(ThriftVenue).where(_.popularity at 3 eqs "hi")""")
    check("""Q(ThriftVenue).where(_.popularity at "a" eqs 3)""")

    // Modify
    check("""Q(ThriftVenue).where(_.legacyid eqs 1).modify(_.legacyid setTo "hi")""")
    // TODO: more

    // whereOpt
    check("""Q(ThriftVenue).whereOpt(Some("hi"))(_.legacyid eqs _)""")

    // Foreign keys
    // first make sure that each type-safe foreign key works as expected
    check("""Q(ThriftVenueClaim).where(_.venueId eqs Q(ThriftVenue).createRecord)""", None)
    check("""Q(ThriftVenueClaim).where(_.venueId neqs Q(ThriftVenue).createRecord)""", None)
    check("""Q(ThriftVenueClaim).where(_.venueId in List(Q(ThriftVenue).createRecord))""", None)
    check("""Q(ThriftVenueClaim).where(_.venueId nin List(Q(ThriftVenue).createRecord))""", None)
    // now check that they reject invalid args
    check("""Q(ThriftVenueClaim).where(_.venueId eqs Q(ThriftTip).createRecord)""")
    check("""Q(ThriftVenueClaim).where(_.venueId neqs Q(ThriftTip).createRecord)""")
    check("""Q(ThriftVenueClaim).where(_.venueId in List(Q(ThriftTip).createRecord))""")
    check("""Q(ThriftVenueClaim).where(_.venueId nin List(Q(ThriftTip).createRecord))""")

    // Can't select array index
    check("""Q(ThriftVenue).where(_.legacyid eqs 1).select(_.tags at 0)""")

    //
    // Phantom type stuff
    //

    check("""Q(ThriftVenue).orderAsc(_.legacyid).orderAsc(_.closed)""")
    check("""Q(ThriftVenue).andAsc(_.legacyid)""")
    check("""Q(ThriftVenue).limit(1).limit(5)""")
    check("""Q(ThriftVenue).limit(1) fetch(5)""")
    check("""Q(ThriftVenue).limit(1) get()""")
    check("""Q(ThriftVenue).skip(3).skip(3)""")
    check("""Q(ThriftVenue).select(_.legacyid).select(_.closed)""")

    // select case class
    check("""Q(ThriftVenue).selectCase(_.legacyid, V2)""")
    check("""Q(ThriftVenue).selectCase(_.legacyid, _.tags, V2)""")

    // Index hints
    check("""Q(ThriftVenue).where(_.legacyid eqs 1).hint(Q(ThriftComment).idx1)""")

    // Modify
    check("""Q(ThriftVenue).limit(1).modify(_.legacyid setTo 1)""")
    check("""Q(ThriftVenue).skip(3).modify(_.legacyid setTo 1)""")
    check("""Q(ThriftVenue).select(_.legacyid).modify(_.legacyid setTo 1)""")

    // Noop
    check("""Q(ThriftVenue).limit(1).noop()""")
    check("""Q(ThriftVenue).skip(3).noop()""")
    check("""Q(ThriftVenue).select(_.legacyid).noop()""")

    // Delete
    check("""Q(ThriftVenue).limit(1) bulkDelete_!!""")
    check("""Q(ThriftVenue).skip(3) bulkDelete_!!""")
    check("""Q(ThriftVenue).select(_.legacyid) bulkDelete_!!""")

    // Or
    check("""Q(ThriftVenue).or(_.where(_.legacyid eqs 1), _.where(_.legacyid eqs 2)).or(_.where(_.closed eqs true), _.where(_.closed exists false))""")
    check("""Q(ThriftVenue).or(_.where(_.legacyid eqs 1), _.where(_.legacyid eqs 2).select(_.venuename))""")
    check("""Q(ThriftVenue).or(_.where(_.legacyid eqs 1), _.where(_.legacyid eqs 2).orderAsc(_.venuename))""")
    check("""Q(ThriftVenue).or(_.where(_.legacyid eqs 1), _.where(_.legacyid eqs 2).limit(10))""")
    check("""Q(ThriftVenue).or(_.where(_.legacyid eqs 1), _.where(_.legacyid eqs 2).skip(10))""")
    check("""OrQuery(Q(ThriftVenue).where(_.legacyid eqs 1), Q(ThriftTip).where(_.legacyid eqs 2))""")

    // Sharding

    check("""Q(ThriftLike).where(_.userid eqs 123).noop()""")
    check("""Q(ThriftLike).where(_.userid eqs 123).count()""")
    check("""Q(ThriftLike).where(_.userid eqs 123).exists()""")
    check("""Q(ThriftLike).where(_.userid eqs 123).fetch()""")
    check("""Q(ThriftLike).where(_.userid eqs 123).bulkDelete_!!!()""")
    check("""Q(ThriftLike).where(_.userid eqs 123).paginate(10)""")
    check("""Q(ThriftLike).withShardKey(_.userid lt 123)""")
    check("""Q(ThriftLike).withShardKey(_.checkin eqs 123)""")
    check("""Q(ThriftLike).where(_.checkin eqs 444).allShards.modify(_.checkin setTo 112).updateOne()""")
    check("""Q(ThriftLike).where(_.checkin eqs 444).allShards.modify(_.checkin setTo 112).and(_.tip unset).updateOne()""")
    check("""Q(ThriftLike).where(_.checkin eqs 444).allShards.findAndModify(_.checkin setTo 112).updateOne()""")
    check("""Q(ThriftLike).where(_.checkin eqs 444).allShards.findAndModify(_.checkin setTo 112).and(_.tip unset).updateOne()""")

    // Indexes

    // TODO(jorge): Reenable type-safe index checking.

    // // Can't say useIndex and then not use that field.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).idIdx).where(_.legacyid eqs 4)""",
    //       Some("found.*EqClause.*required.*_id"))
    // // Can't use where with an IndexScan'ing operation.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).idIdx).where(_.id)(_ after new DateTime())""",
    //       Some("do not conform to method where.*com.foursquare.rogue.Indexable"))
    // // But you can use iscan with an IndexScan'ing operation.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).idIdx).iscan(_.id)(_ after new DateTime())""",
    //       None)

    // // Can't skip past the first field in an index.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).idIdx).rangeScan(_.id)""",
    //       Some("(could not find implicit value for parameter ev|Cannot prove that).*com.foursquare.rogue.UsedIndex"))

    // // Can't skip past the first field in an index.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).mayorIdIdx).rangeScan(_.mayor).iscan(_.id)(_ eqs new ObjectId())""",
    //       Some("(could not find implicit value for parameter ev|Cannot prove that).*com.foursquare.rogue.UsedIndex"))

    // // If first column is index-scanned, other fields must be marked as iscan too.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).mayorIdIdx).iscan(_.mayor)(_ lt 10).where(_.id)(_ eqs new ObjectId())""",
    //       Some("(could not find implicit value for parameter ev|Cannot prove that).*com.foursquare.rogue.Indexable"))
    // // Query should compile fine when the second clause is marked as iscan.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).mayorIdIdx).iscan(_.mayor)(_ lt 10).iscan(_.id)(_ eqs new ObjectId())""",
    //       None)

    // // If you rangeScan past a column, you must iscan all index fields after.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).mayorIdClosedIdx).where(_.mayor)(_ eqs 10).rangeScan(_.id).where(_.closed)(_ eqs true)""",
    //       Some("(could not find implicit value for parameter ev|Cannot prove that).*com.foursquare.rogue.Indexable"))
    // // Version of the above with an iscan of later fields.
    // check("""Q(ThriftVenue).useIndex(Q(ThriftVenue).mayorIdClosedIdx).where(_.mayor)(_ eqs 10).rangeScan(_.id).iscan(_.closed)(_ eqs true)""",
    //       None)
  }
}
