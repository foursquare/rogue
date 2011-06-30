// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

import com.foursquare.rogue.Rogue._

import java.util.regex.Pattern
import net.liftweb.mongodb.record._
import net.liftweb.mongodb.record.field._
import net.liftweb.record.field._
import net.liftweb.record._
import org.bson.types._
import org.joda.time.{DateTime, DateTimeZone}

import org.junit._
import org.specs.SpecsMatchers

/////////////////////////////////////////////////
// Sample records for testing
/////////////////////////////////////////////////

object VenueStatus extends Enumeration {
  val open = Value("Open")
  val closed = Value("Closed")
}

class Venue extends MongoRecord[Venue] with MongoId[Venue] {
  def meta = Venue
  object legacyid extends LongField(this) { override def name = "legid" }
  object userid extends LongField(this)
  object venuename extends StringField(this, 255)
  object mayor extends LongField(this)
  object mayor_count extends LongField(this)
  object closed extends BooleanField(this)
  object tags extends MongoListField[Venue, String](this)
  object popularity extends MongoListField[Venue, Long](this)
  object categories extends MongoListField[Venue, ObjectId](this)
  object geolatlng extends MongoCaseClassField[Venue, LatLong](this) { override def name = "latlng" }
  object last_updated extends DateTimeField(this)
  object status extends EnumNameField(this, VenueStatus) { override def name = "status" }
}
object Venue extends Venue with MongoMetaRecord[Venue] {
  object CustomIndex extends IndexModifier("custom")
  val idIdx = Venue.index(_._id, Asc)
  val legIdx = Venue.index(_.legacyid, Desc)
  val geoIdx = Venue.index(_.geolatlng, TwoD)
  val geoCustomIdx = Venue.index(_.geolatlng, CustomIndex, _.tags, Asc)
}

object ClaimStatus extends Enumeration {
  val pending = Value("Pending approval")
  val approved = Value("Approved")
}

class VenueClaim extends MongoRecord[VenueClaim] with MongoId[VenueClaim] {
  def meta = VenueClaim
  object userid extends LongField(this) { override def name = "uid" }
  object status extends EnumNameField(this, ClaimStatus)
}
object VenueClaim extends VenueClaim with MongoMetaRecord[VenueClaim]


case class OneComment(timestamp: String, userid: Long, comment: String)
class Comment extends MongoRecord[Comment] with MongoId[Comment] {
  def meta = Comment
  object comments extends MongoCaseClassListField[Comment, OneComment](this)
}
object Comment extends Comment with MongoMetaRecord[Comment]

class Tip extends MongoRecord[Tip] with MongoId[Tip] {
  def meta = Tip
  object legacyid extends LongField(this) { override def name = "legid" }
  object counts extends MongoMapField[Tip, Long](this)
}
object Tip extends Tip with MongoMetaRecord[Tip]

object ConsumerPrivilege extends Enumeration {
  val awardBadges = Value("Award badges")
}

class OAuthConsumer extends MongoRecord[OAuthConsumer] with MongoId[OAuthConsumer] {
  def meta = OAuthConsumer
  object privileges extends MongoListField[OAuthConsumer, ConsumerPrivilege.Value](this)
}
object OAuthConsumer extends OAuthConsumer with MongoMetaRecord[OAuthConsumer]


/////////////////////////////////////////////////
// Actual tests
/////////////////////////////////////////////////

class QueryTest extends SpecsMatchers {

  @Test
  def testProduceACorrectJSONQueryString {
    val d1 = new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)
    val d2 = new DateTime(2010, 5, 2, 0, 0, 0, 0, DateTimeZone.UTC)
    val oid1 = new ObjectId(d1.toDate, 0, 0)
    val oid2 = new ObjectId(d2.toDate, 0, 0)
    val oid = new ObjectId

    // eqs
    Venue where (_.mayor eqs 1)               toString() must_== """{ "mayor" : 1}"""
    Venue where (_.venuename eqs "Starbucks") toString() must_== """{ "venuename" : "Starbucks"}"""
    Venue where (_.closed eqs true)           toString() must_== """{ "closed" : true}"""
    Venue where (_._id eqs oid)               toString() must_== ("""{ "_id" : { "$oid" : "%s"}}""" format oid.toString)
    VenueClaim where (_.status eqs ClaimStatus.approved) toString() must_== """{ "status" : "Approved"}"""

    // neq,lt,gt
    Venue where (_.mayor_count neqs 5) toString() must_== """{ "mayor_count" : { "$ne" : 5}}"""
    Venue where (_.mayor_count < 5)   toString() must_== """{ "mayor_count" : { "$lt" : 5}}"""
    Venue where (_.mayor_count lt 5)  toString() must_== """{ "mayor_count" : { "$lt" : 5}}"""
    Venue where (_.mayor_count <= 5)  toString() must_== """{ "mayor_count" : { "$lte" : 5}}"""
    Venue where (_.mayor_count lte 5) toString() must_== """{ "mayor_count" : { "$lte" : 5}}"""
    Venue where (_.mayor_count > 5)   toString() must_== """{ "mayor_count" : { "$gt" : 5}}"""
    Venue where (_.mayor_count gt 5)  toString() must_== """{ "mayor_count" : { "$gt" : 5}}"""
    Venue where (_.mayor_count >= 5)  toString() must_== """{ "mayor_count" : { "$gte" : 5}}"""
    Venue where (_.mayor_count gte 5) toString() must_== """{ "mayor_count" : { "$gte" : 5}}"""
    Venue where (_.mayor_count between (3, 5)) toString() must_== """{ "mayor_count" : { "$gte" : 3 , "$lte" : 5}}"""
    VenueClaim where (_.status neqs ClaimStatus.approved) toString() must_== """{ "status" : { "$ne" : "Approved"}}"""

    // in,nin
    Venue where (_.legacyid in List(123, 456)) toString() must_== """{ "legid" : { "$in" : [ 123 , 456]}}"""
    Venue where (_.venuename nin List("Starbucks", "Whole Foods")) toString() must_== """{ "venuename" : { "$nin" : [ "Starbucks" , "Whole Foods"]}}"""
    VenueClaim where (_.status in List(ClaimStatus.approved, ClaimStatus.pending))  toString() must_== """{ "status" : { "$in" : [ "Approved" , "Pending approval"]}}"""
    VenueClaim where (_.status nin List(ClaimStatus.approved, ClaimStatus.pending)) toString() must_== """{ "status" : { "$nin" : [ "Approved" , "Pending approval"]}}"""

    // exists
    Venue where (_._id exists true) toString() must_== """{ "_id" : { "$exists" : true}}"""

    // startsWith, regex
    Venue where (_.venuename startsWith "Starbucks") toString() must_== """{ "venuename" : { "$regex" : "^\\QStarbucks\\E" , "$options" : ""}}"""
    Venue where (_.venuename regexWarningNotIndexed Pattern.compile("Star.*")) toString() must_== """{ "venuename" : { "$regex" : "Star.*" , "$options" : ""}}"""

    // all, in, size, contains, at
    Venue where (_.tags all List("db", "ka"))   toString() must_== """{ "tags" : { "$all" : [ "db" , "ka"]}}"""
    Venue where (_.tags in  List("db", "ka"))   toString() must_== """{ "tags" : { "$in" : [ "db" , "ka"]}}"""
    Venue where (_.tags size 3)                 toString() must_== """{ "tags" : { "$size" : 3}}"""
    Venue where (_.tags contains "karaoke")     toString() must_== """{ "tags" : "karaoke"}"""
    Venue where (_.popularity contains 3)       toString() must_== """{ "popularity" : 3}"""
    Venue where (_.popularity at 0 eqs 3)       toString() must_== """{ "popularity.0" : 3}"""
    Venue where (_.categories at 0 eqs oid)     toString() must_== """{ "categories.0" : { "$oid" : "%s"}}""".format(oid.toString)
    Venue where (_.tags at 0 startsWith "kara") toString() must_== """{ "tags.0" : { "$regex" : "^\\Qkara\\E" , "$options" : ""}}"""
    // alternative syntax
    Venue where (_.tags idx 0 startsWith "kara") toString() must_== """{ "tags.0" : { "$regex" : "^\\Qkara\\E" , "$options" : ""}}"""

    // maps
    Tip where (_.counts at "foo" eqs 3) toString() must_== """{ "counts.foo" : 3}"""

    // near
    Venue where (_.geolatlng near (39.0, -74.0, Degrees(0.2)))     toString() must_== """{ "latlng" : { "$near" : [ 39.0 , -74.0 , 0.2]}}"""
    Venue where (_.geolatlng withinCircle(1.0, 2.0, Degrees(0.3))) toString() must_== """{ "latlng" : { "$within" : { "$center" : [ [ 1.0 , 2.0] , 0.3]}}}"""
    Venue where (_.geolatlng withinBox(1.0, 2.0, 3.0, 4.0))        toString() must_== """{ "latlng" : { "$within" : { "$box" : [ [ 1.0 , 2.0] , [ 3.0 , 4.0]]}}}"""
    Venue where (_.geolatlng eqs (45.0, 50.0)) toString() must_== """{ "latlng" : [ 45.0 , 50.0]}"""
    Venue where (_.geolatlng neqs (31.0, 23.0)) toString()  must_== """{ "latlng" : { "$ne" : [ 31.0 , 23.0]}}"""
    Venue where (_.geolatlng eqs LatLong(45.0, 50.0)) toString() must_== """{ "latlng" : [ 45.0 , 50.0]}"""
    Venue where (_.geolatlng neqs LatLong(31.0, 23.0)) toString()  must_== """{ "latlng" : { "$ne" : [ 31.0 , 23.0]}}"""

    // ObjectId before, after, between
    Venue where (_._id before d2)        toString() must_== """{ "_id" : { "$lt" : { "$oid" : "%s"}}}""".format(oid2.toString)
    Venue where (_._id after d1)         toString() must_== """{ "_id" : { "$gt" : { "$oid" : "%s"}}}""".format(oid1.toString)
    Venue where (_._id between (d1, d2)) toString() must_== """{ "_id" : { "$gt" : { "$oid" : "%s"} , "$lt" : { "$oid" : "%s"}}}""".format(oid1.toString, oid2.toString)

    // DateTime before, after, between
    Venue where (_.last_updated before d2)        toString() must_== """{ "last_updated" : { "$lt" : { "$date" : "2010-05-02T00:00:00Z"}}}"""
    Venue where (_.last_updated after d1)         toString() must_== """{ "last_updated" : { "$gt" : { "$date" : "2010-05-01T00:00:00Z"}}}"""
    Venue where (_.last_updated between (d1, d2)) toString() must_== """{ "last_updated" : { "$gt" : { "$date" : "2010-05-01T00:00:00Z"} , "$lt" : { "$date" : "2010-05-02T00:00:00Z"}}}"""

    // Case class list field
    Comment where (_.comments.unsafeField[Int]("z") eqs 123) toString() must_== """{ "comments.z" : 123}"""
    Comment where (_.comments.unsafeField[String]("comment") eqs "hi") toString() must_== """{ "comments.comment" : "hi"}"""

    // Enumeration list
    OAuthConsumer where (_.privileges contains ConsumerPrivilege.awardBadges) toString() must_== """{ "privileges" : "Award badges"}"""
    OAuthConsumer where (_.privileges at 0 eqs ConsumerPrivilege.awardBadges) toString() must_== """{ "privileges.0" : "Award badges"}"""

    // Field type
    Venue where (_.legacyid hastype MongoType.String) toString() must_== """{ "legid" : { "$type" : 2}}"""

    // Modulus
    Venue where (_.legacyid mod (5, 1)) toString() must_== """{ "legid" : { "$mod" : [ 5 , 1]}}"""

    // compound queries
    Venue where (_.mayor eqs 1) and (_.tags contains "karaoke") toString() must_== """{ "mayor" : 1 , "tags" : "karaoke"}"""
    Venue where (_.mayor eqs 1) and (_.mayor_count eqs 5)       toString() must_== """{ "mayor" : 1 , "mayor_count" : 5}"""
    Venue where (_.mayor eqs 1) and (_.mayor_count lt 5)        toString() must_== """{ "mayor" : 1 , "mayor_count" : { "$lt" : 5}}"""
    Venue where (_.mayor eqs 1) and (_.mayor_count gt 3) and (_.mayor_count lt 5) toString() must_== """{ "mayor" : 1 , "mayor_count" : { "$lt" : 5 , "$gt" : 3}}"""

    // queries with no clauses
    metaRecordToQueryBuilder(Venue) toString() must_== "{ }"
    Venue orderDesc(_._id) toString() must_== """{ } order by { "_id" : -1}"""

    // ordered queries
    Venue where (_.mayor eqs 1) orderAsc(_.legacyid) toString() must_== """{ "mayor" : 1} order by { "legid" : 1}"""
    Venue where (_.mayor eqs 1) orderDesc(_.legacyid) andAsc(_.userid) toString() must_== """{ "mayor" : 1} order by { "legid" : -1 , "userid" : 1}"""

    // select queries
    Venue where (_.mayor eqs 1) select(_.legacyid) toString() must_== """{ "mayor" : 1} select { "legid" : 1}"""
    Venue where (_.mayor eqs 1) select(_.legacyid, _.userid) toString() must_== """{ "mayor" : 1} select { "legid" : 1 , "userid" : 1}"""
    Venue where (_.mayor eqs 1) select(_.legacyid, _.userid, _.mayor) toString() must_== """{ "mayor" : 1} select { "legid" : 1 , "userid" : 1 , "mayor" : 1}"""
    Venue where (_.mayor eqs 1) select(_.legacyid, _.userid, _.mayor, _.mayor_count) toString() must_== """{ "mayor" : 1} select { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1}"""
    Venue where (_.mayor eqs 1) select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed) toString() must_== """{ "mayor" : 1} select { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1 , "closed" : 1}"""
    Venue where (_.mayor eqs 1) select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags) toString() must_== """{ "mayor" : 1} select { "legid" : 1 , "userid" : 1 , "mayor" : 1 , "mayor_count" : 1 , "closed" : 1 , "tags" : 1}"""

    // select subfields
    Tip where (_.legacyid eqs 1) select (_.counts at "foo") toString() must_== """{ "legid" : 1} select { "counts.foo" : 1}"""
    Venue where (_.legacyid eqs 1) select (_.geolatlng.unsafeField[Double]("lat")) toString() must_== """{ "legid" : 1} select { "latlng.lat" : 1}"""
    // TODO: Comment select(_.comments.unsafeField[Long]("userid")) toString() must_== """{ } select { "comments.userid" : 1}"""
    // won't compile (by design)
    // Venue where (_.mayor eqs 1) select(_.tags at 0) toString() must_== """{ "mayor" : 1} select { "tags.0" : 1}"""

    // empty queries
    Venue where (_.mayor in List()) toString() must_== "empty query"
    Venue where (_.tags in List()) toString() must_== "empty query"
    Venue where (_.tags all List()) toString() must_== "empty query"
    Comment where (_.comments in List()) toString() must_== "empty query"
    Venue where (_.tags contains "karaoke") and (_.mayor in List()) toString() must_== "empty query"
    Venue where (_.mayor in List()) and (_.tags contains "karaoke") toString() must_== "empty query"

    // out of order and doesn't screw up earlier params
    Venue limit(10) where (_.mayor eqs 1) toString() must_== """{ "mayor" : 1} limit 10"""
    Venue orderDesc(_._id) and (_.mayor eqs 1) toString() must_== """{ "mayor" : 1} order by { "_id" : -1}"""
    Venue orderDesc(_._id) skip(3) and (_.mayor eqs 1) toString() must_== """{ "mayor" : 1} order by { "_id" : -1} skip 3"""

    // Scan should be the same as and/where
    Venue where (_.mayor eqs 1) scan (_.tags contains "karaoke") toString() must_== """{ "mayor" : 1 , "tags" : "karaoke"}"""
    Venue scan (_.mayor eqs 1) and (_.mayor_count eqs 5)         toString() must_== """{ "mayor" : 1 , "mayor_count" : 5}"""
    Venue scan (_.mayor eqs 1) scan (_.mayor_count lt 5)         toString() must_== """{ "mayor" : 1 , "mayor_count" : { "$lt" : 5}}"""
    Venue where (_.mayor in List()) scan (_.mayor_count eqs 5)   toString() must_== "empty query"
  }

  @Test
  def testModifyQueryShouldProduceACorrectJSONQueryString {
    val d1 = new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)

    val query = """{ "legid" : 1} modify with """
    Venue where (_.legacyid eqs 1) modify (_.venuename setTo "fshq") toString() must_== query + """{ "$set" : { "venuename" : "fshq"}}"""
    Venue where (_.legacyid eqs 1) modify (_.mayor_count setTo 3)    toString() must_== query + """{ "$set" : { "mayor_count" : 3}}"""
    Venue where (_.legacyid eqs 1) modify (_.mayor_count unset)      toString() must_== query + """{ "$unset" : { "mayor_count" : 1}}"""

    // Numeric
    Venue where (_.legacyid eqs 1) modify (_.mayor_count inc 3) toString() must_== query + """{ "$inc" : { "mayor_count" : 3}}"""

    // Enumeration
    val query2 = "{ \"uid\" : 1} modify with "
    VenueClaim where (_.userid eqs 1) modify (_.status setTo ClaimStatus.approved) toString() must_== query2 + """{ "$set" : { "status" : "Approved"}}"""

    // Calendar
    Venue where (_.legacyid eqs 1) modify (_.last_updated setTo d1) toString() must_== query + """{ "$set" : { "last_updated" : { "$date" : "2010-05-01T00:00:00Z"}}}"""
    Venue where (_.legacyid eqs 1) modify (_.last_updated setTo d1.toGregorianCalendar) toString() must_== query + """{ "$set" : { "last_updated" : { "$date" : "2010-05-01T00:00:00Z"}}}"""

    // LatLong
    val ll = LatLong(37.4, -73.9)
    Venue where (_.legacyid eqs 1) modify (_.geolatlng setTo ll) toString() must_== query + """{ "$set" : { "latlng" : [ 37.4 , -73.9]}}"""

    // Lists
    Venue where (_.legacyid eqs 1) modify (_.popularity setTo List(5))       toString() must_== query + """{ "$set" : { "popularity" : [ 5]}}"""
    Venue where (_.legacyid eqs 1) modify (_.popularity push 5)              toString() must_== query + """{ "$push" : { "popularity" : 5}}"""
    Venue where (_.legacyid eqs 1) modify (_.tags pushAll List("a", "b"))    toString() must_== query + """{ "$pushAll" : { "tags" : [ "a" , "b"]}}"""
    Venue where (_.legacyid eqs 1) modify (_.tags addToSet "a")              toString() must_== query + """{ "$addToSet" : { "tags" : "a"}}"""
    Venue where (_.legacyid eqs 1) modify (_.popularity addToSet List(1, 2)) toString() must_== query + """{ "$addToSet" : { "popularity" : { "$each" : [ 1 , 2]}}}"""
    Venue where (_.legacyid eqs 1) modify (_.tags popFirst)                  toString() must_== query + """{ "$pop" : { "tags" : -1}}"""
    Venue where (_.legacyid eqs 1) modify (_.tags popLast)                   toString() must_== query + """{ "$pop" : { "tags" : 1}}"""
    Venue where (_.legacyid eqs 1) modify (_.tags pull "a")                  toString() must_== query + """{ "$pull" : { "tags" : "a"}}"""
    Venue where (_.legacyid eqs 1) modify (_.popularity pullAll List(2, 3))  toString() must_== query + """{ "$pullAll" : { "popularity" : [ 2 , 3]}}"""
    Venue where (_.legacyid eqs 1) modify (_.popularity at 0 inc 1)         toString() must_== query + """{ "$inc" : { "popularity.0" : 1}}"""
    // alternative syntax
    Venue where (_.legacyid eqs 1) modify (_.popularity idx 0 inc 1)         toString() must_== query + """{ "$inc" : { "popularity.0" : 1}}"""

    // Enumeration list
    OAuthConsumer modify (_.privileges addToSet ConsumerPrivilege.awardBadges) toString() must_== """{ } modify with { "$addToSet" : { "privileges" : "Award badges"}}"""

    // Map
    val m = Map("foo" -> 1L)
    Tip where (_.legacyid eqs 1) modify (_.counts setTo m)          toString() must_== query + """{ "$set" : { "counts" : { "foo" : 1}}}"""
    Tip where (_.legacyid eqs 1) modify (_.counts at "foo" setTo 3) toString() must_== query + """{ "$set" : { "counts.foo" : 3}}"""
    Tip where (_.legacyid eqs 1) modify (_.counts at "foo" inc 5)   toString() must_== query + """{ "$inc" : { "counts.foo" : 5}}"""
    Tip where (_.legacyid eqs 1) modify (_.counts at "foo" unset)   toString() must_== query + """{ "$unset" : { "counts.foo" : 1}}"""
    Tip where (_.legacyid eqs 1) modify (_.counts setTo Map("foo" -> 3, "bar" -> 5)) toString() must_== query + """{ "$set" : { "counts" : { "foo" : 3 , "bar" : 5}}}"""

    // Multiple updates
    Venue where (_.legacyid eqs 1) modify (_.venuename setTo "fshq") and (_.mayor_count setTo 3) toString() must_== query + """{ "$set" : { "mayor_count" : 3 , "venuename" : "fshq"}}"""
    Venue where (_.legacyid eqs 1) modify (_.venuename setTo "fshq") and (_.mayor_count inc 1)   toString() must_== query + """{ "$set" : { "venuename" : "fshq"} , "$inc" : { "mayor_count" : 1}}"""
    Venue where (_.legacyid eqs 1) modify (_.venuename setTo "fshq") and (_.mayor_count setTo 3) and (_.mayor_count inc 1) toString() must_== query + """{ "$set" : { "mayor_count" : 3 , "venuename" : "fshq"} , "$inc" : { "mayor_count" : 1}}"""
    Venue where (_.legacyid eqs 1) modify (_.popularity addToSet 3) and (_.tags addToSet List("a", "b")) toString() must_== query + """{ "$addToSet" : { "tags" : { "$each" : [ "a" , "b"]} , "popularity" : 3}}"""

    // Empty query
    Venue where (_.mayor in List()) modify (_.venuename setTo "fshq") toString() must_== "empty modify query"

    // Noop query
    Venue where (_.legacyid eqs 1) noop() toString() must_== query + "{ }"
    Venue where (_.legacyid eqs 1) noop() modify (_.venuename setTo "fshq") toString() must_== query + """{ "$set" : { "venuename" : "fshq"}}"""
    Venue where (_.legacyid eqs 1) noop() and (_.venuename setTo "fshq") toString() must_== query + """{ "$set" : { "venuename" : "fshq"}}"""
  }

  @Test
  def testGenericSetField {
    def setField[T, M <: MongoRecord[M]](field: Field[T, Venue], value: T) =
      (v: Venue) => fieldToModifyField(field) setTo value

    val venue = new Venue().geolatlng(LatLong(37.4, -73.9)).legacyid(2).status(VenueStatus.closed)
    val fields: List[Field[_, Venue]] = List(venue.geolatlng, venue.legacyid, venue.status)
    val query = Venue where (_.legacyid eqs 1) modify (_.venuename setTo "Starbucks")
    val query2 = fields.foldLeft(query){ case (q, f) => {
      val v = f.valueBox.open_!
      q and setField(f, v)
    }}
    query2 toString() must_== """{ "legid" : 1} modify with { "$set" : { "status" : "Closed" , "legid" : 2 , "latlng" : [ 37.4 , -73.9] , "venuename" : "Starbucks"}}"""
  }

  @Test
  def testProduceACorrectSignatureString {
    val d1 = new DateTime(2010, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)
    val d2 = new DateTime(2010, 5, 2, 0, 0, 0, 0, DateTimeZone.UTC)
    val oid = new ObjectId

    // basic ops
    Venue where (_.mayor eqs 1)               signature() must_== """{ "mayor" : 0}"""
    Venue where (_.venuename eqs "Starbucks") signature() must_== """{ "venuename" : 0}"""
    Venue where (_.closed eqs true)           signature() must_== """{ "closed" : 0}"""
    Venue where (_._id eqs oid)               signature() must_== """{ "_id" : 0}"""
    VenueClaim where (_.status eqs ClaimStatus.approved) signature() must_== """{ "status" : 0}"""
    Venue where (_.mayor_count gte 5) signature() must_== """{ "mayor_count" : { "$gte" : 0}}"""
    VenueClaim where (_.status neqs ClaimStatus.approved) signature() must_== """{ "status" : { "$ne" : 0}}"""
    Venue where (_.legacyid in List(123, 456)) signature() must_== """{ "legid" : { "$in" : 0}}"""
    Venue where (_._id exists true) signature() must_== """{ "_id" : { "$exists" : 0}}"""
    Venue where (_.venuename startsWith "Starbucks") signature() must_== """{ "venuename" : 0}"""

    // list
    Venue where (_.tags all List("db", "ka"))   signature() must_== """{ "tags" : { "$all" : 0}}"""
    Venue where (_.tags in  List("db", "ka"))   signature() must_== """{ "tags" : { "$in" : 0}}"""
    Venue where (_.tags size 3)                 signature() must_== """{ "tags" : { "$size" : 0}}"""
    Venue where (_.tags contains "karaoke")     signature() must_== """{ "tags" : 0}"""
    Venue where (_.popularity contains 3)       signature() must_== """{ "popularity" : 0}"""
    Venue where (_.popularity at 0 eqs 3)       signature() must_== """{ "popularity.0" : 0}"""
    Venue where (_.categories at 0 eqs oid)     signature() must_== """{ "categories.0" : 0}"""
    Venue where (_.tags at 0 startsWith "kara") signature() must_== """{ "tags.0" : 0}"""
    Venue where (_.tags idx 0 startsWith "kara") signature() must_== """{ "tags.0" : 0}"""

    // map
    Tip where (_.counts at "foo" eqs 3) signature() must_== """{ "counts.foo" : 0}"""

    // near
    Venue where (_.geolatlng near (39.0, -74.0, Degrees(0.2)))     signature() must_== """{ "latlng" : { "$near" : 0}}"""
    Venue where (_.geolatlng withinCircle(1.0, 2.0, Degrees(0.3))) signature() must_== """{ "latlng" : { "$within" : { "$center" : 0}}}"""
    Venue where (_.geolatlng withinBox(1.0, 2.0, 3.0, 4.0))        signature() must_== """{ "latlng" : { "$within" : { "$box" : 0}}}"""
    Venue where (_.geolatlng eqs (45.0, 50.0)) signature() must_== """{ "latlng" : 0}"""

    // id, date range
    Venue where (_._id before d2) signature() must_== """{ "_id" : { "$lt" : 0}}"""
    Venue where (_.last_updated before d2) signature() must_== """{ "last_updated" : { "$lt" : 0}}"""

    // Case class list field
    Comment where (_.comments.unsafeField[Int]("z") eqs 123) signature() must_== """{ "comments.z" : 0}"""
    Comment where (_.comments.unsafeField[String]("comment") eqs "hi") signature() must_== """{ "comments.comment" : 0}"""

    // Enumeration list
    OAuthConsumer where (_.privileges contains ConsumerPrivilege.awardBadges) signature() must_== """{ "privileges" : 0}"""
    OAuthConsumer where (_.privileges at 0 eqs ConsumerPrivilege.awardBadges) signature() must_== """{ "privileges.0" : 0}"""

    // Field type
    Venue where (_.legacyid hastype MongoType.String) signature() must_== """{ "legid" : { "$type" : 0}}"""

    // Modulus
    Venue where (_.legacyid mod (5, 1)) signature() must_== """{ "legid" : { "$mod" : 0}}"""

    // compound queries
    Venue where (_.mayor eqs 1) and (_.tags contains "karaoke") signature() must_== """{ "mayor" : 0 , "tags" : 0}"""
    Venue where (_.mayor eqs 1) and (_.mayor_count gt 3) and (_.mayor_count lt 5) signature() must_== """{ "mayor" : 0 , "mayor_count" : { "$lt" : 0 , "$gt" : 0}}"""

    // queries with no clauses
    metaRecordToQueryBuilder(Venue) signature() must_== "{ }"
    Venue orderDesc(_._id) signature() must_== """{ } order by { "_id" : -1}"""

    // ordered queries
    Venue where (_.mayor eqs 1) orderAsc(_.legacyid) signature() must_== """{ "mayor" : 0} order by { "legid" : 1}"""
    Venue where (_.mayor eqs 1) orderDesc(_.legacyid) andAsc(_.userid) signature() must_== """{ "mayor" : 0} order by { "legid" : -1 , "userid" : 1}"""

    // select queries
    Venue where (_.mayor eqs 1) select(_.legacyid) signature() must_== """{ "mayor" : 0}"""

    // empty queries
    Venue where (_.mayor in List()) signature() must_== "empty query"
    Venue where (_.tags all List()) signature() must_== "empty query"
    Venue where (_.tags contains "karaoke") and (_.mayor in List()) signature() must_== "empty query"
    Venue where (_.mayor in List()) and (_.tags contains "karaoke") signature() must_== "empty query"

    // Scan should be the same as and/where
    Venue where (_.mayor eqs 1) scan (_.tags contains "karaoke") signature() must_== """{ "mayor" : 0 , "tags" : 0}"""
  }

  @Test
  def testHints {
    Venue where (_.legacyid eqs 1) hint (Venue.idIdx) toString() must_== """{ "legid" : 1} hint { "_id" : 1}"""
    Venue where (_.legacyid eqs 1) hint (Venue.legIdx) toString() must_== """{ "legid" : 1} hint { "legid" : -1}"""
    Venue where (_.legacyid eqs 1) hint (Venue.geoIdx) toString() must_== """{ "legid" : 1} hint { "latlng" : "2d"}"""
    Venue where (_.legacyid eqs 1) hint (Venue.geoCustomIdx) toString() must_== """{ "legid" : 1} hint { "latlng" : "custom" , "tags" : 1}"""
  }
}
