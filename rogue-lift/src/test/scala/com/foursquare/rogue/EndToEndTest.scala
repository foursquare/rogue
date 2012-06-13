// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

import com.foursquare.rogue.LiftRogue._
import com.foursquare.rogue.Rogue.Iter._
import com.mongodb.ReadPreference

import java.util.regex.Pattern
import net.liftweb.common.{Box, Empty, Full}
import org.bson.types.ObjectId
import org.junit.{Before, After, Ignore, Test}
import org.specs.SpecsMatchers

/**
 * Contains tests that test the interaction of Rogue with a real mongo.
 */
class EndToEndTest extends SpecsMatchers {
  def baseTestVenue(): Venue = {
    Venue.createRecord
         .legacyid(123)
         .userid(456)
         .venuename("test venue")
         .mayor(789)
         .mayor_count(3)
         .closed(false)
         .popularity(List(1L, 2L, 3L))
         .categories(List(new ObjectId()))
         .geolatlng(LatLong(40.73, -73.98))
         .status(VenueStatus.open)
         .claims(List(VenueClaimBson.createRecord.userid(1234).status(ClaimStatus.pending),
                      VenueClaimBson.createRecord.userid(5678).status(ClaimStatus.approved)))
         .lastClaim(VenueClaimBson.createRecord.userid(5678).status(ClaimStatus.approved))
  }

  def baseTestVenueClaim(vid: ObjectId): VenueClaim = {
    VenueClaim.createRecord
              .venueid(vid)
              .userid(123)
              .status(ClaimStatus.approved)
  }

  def baseTestTip(): Tip = {
    Tip.createRecord
       .legacyid(234)
       .counts(Map("foo" -> 1L,
                   "bar" -> 2L))
  }

  @Before
  def setupMongoConnection: Unit = {
    RogueTestMongo.connectToMongo
  }

  @After
  def cleanupTestData: Unit = {
    Venue.bulkDelete_!!!
    Venue.count must_== 0

    VenueClaim.bulkDelete_!!!
    VenueClaim.count must_== 0

    RogueTestMongo.disconnectFromMongo
  }

  @Test
  def eqsTests: Unit = {
    val v = baseTestVenue().save
    val vc = baseTestVenueClaim(v.id).save

    // eqs
    metaRecordToQueryBuilder(Venue).where(_._id eqs v.id).fetch().map(_.id)                         must_== List(v.id)
    Venue.where(_.mayor eqs v.mayor.value).fetch().map(_.id)              must_== List(v.id)
    Venue.where(_.mayor eqs v.mayor.value).fetch().map(_.id)              must_== List(v.id)
    Venue.where(_.venuename eqs v.venuename.value).fetch().map(_.id)      must_== List(v.id)
    Venue.where(_.closed eqs false).fetch().map(_.id)                     must_== List(v.id)

    Venue.where(_.mayor eqs 432432).fetch().map(_.id)                     must_== Nil
    Venue.where(_.closed eqs true).fetch().map(_.id)                      must_== Nil

    VenueClaim.where(_.status eqs ClaimStatus.approved).fetch().map(_.id) must_== List(vc.id)
    VenueClaim.where(_.venueid eqs v.id).fetch().map(_.id)                must_== List(vc.id)
    VenueClaim.where(_.venueid eqs v).fetch().map(_.id)                   must_== List(vc.id)
  }

  @Test
  def testInequalityQueries: Unit = {
    val v = baseTestVenue().save
    val vc = baseTestVenueClaim(v.id).save

    // neq,lt,gt, where the lone Venue has mayor_count=3, and the only
    // VenueClaim has status approved.
    Venue.where(_.mayor_count neqs 5).fetch().map(_.id)                     must_== List(v.id)
    Venue.where(_.mayor_count < 5).fetch().map(_.id)                        must_== List(v.id)
    Venue.where(_.mayor_count lt 5).fetch().map(_.id)                       must_== List(v.id)
    Venue.where(_.mayor_count <= 5).fetch().map(_.id)                       must_== List(v.id)
    Venue.where(_.mayor_count lte 5).fetch().map(_.id)                      must_== List(v.id)
    Venue.where(_.mayor_count > 5).fetch().map(_.id)                        must_== Nil
    Venue.where(_.mayor_count gt 5).fetch().map(_.id)                       must_== Nil
    Venue.where(_.mayor_count >= 5).fetch().map(_.id)                       must_== Nil
    Venue.where(_.mayor_count gte 5).fetch().map(_.id)                      must_== Nil
    Venue.where(_.mayor_count between (3, 5)).fetch().map(_.id)             must_== List(v.id)
    VenueClaim.where (_.status neqs ClaimStatus.approved).fetch().map(_.id) must_== Nil
    VenueClaim.where (_.status neqs ClaimStatus.pending).fetch().map(_.id)  must_== List(vc.id)
  }

  @Test
  def selectQueries: Unit = {
    val v = baseTestVenue().save

    val base = Venue.where(_._id eqs v.id)
    base.select(_.legacyid).fetch() must_== List(v.legacyid.value)
    base.select(_.legacyid, _.userid).fetch() must_== List((v.legacyid.value, v.userid.value))
    base.select(_.legacyid, _.userid, _.mayor).fetch() must_== List((v.legacyid.value, v.userid.value, v.mayor.value))
    base.select(_.legacyid, _.userid, _.mayor, _.mayor_count).fetch() must_== List((v.legacyid.value, v.userid.value, v.mayor.value, v.mayor_count.value))
    base.select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed).fetch() must_== List((v.legacyid.value, v.userid.value, v.mayor.value, v.mayor_count.value, v.closed.value))
    base.select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags).fetch() must_== List((v.legacyid.value, v.userid.value, v.mayor.value, v.mayor_count.value, v.closed.value, v.tags.value))
  }

  @Test
  def selectEnum: Unit = {
    val v = baseTestVenue().save
    Venue.where(_._id eqs v.id).select(_.status).fetch() must_== List(VenueStatus.open)
  }

  @Test
  def selectCaseQueries: Unit = {
    val v = baseTestVenue().save

    val base = Venue.where(_._id eqs v.id)
    base.selectCase(_.legacyid, V1).fetch() must_== List(V1(v.legacyid.value))
    base.selectCase(_.legacyid, _.userid, V2).fetch() must_== List(V2(v.legacyid.value, v.userid.value))
    base.selectCase(_.legacyid, _.userid, _.mayor, V3).fetch() must_== List(V3(v.legacyid.value, v.userid.value, v.mayor.value))
    base.selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, V4).fetch() must_== List(V4(v.legacyid.value, v.userid.value, v.mayor.value, v.mayor_count.value))
    base.selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, V5).fetch() must_== List(V5(v.legacyid.value, v.userid.value, v.mayor.value, v.mayor_count.value, v.closed.value))
    base.selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags, V6).fetch() must_== List(V6(v.legacyid.value, v.userid.value, v.mayor.value, v.mayor_count.value, v.closed.value, v.tags.value))
  }

  @Test
  def selectSubfieldQueries: Unit = {
    val v = baseTestVenue().save
    val t = baseTestTip().save

    // select subfields
    Tip.where(_._id eqs t.id).select(_.counts at "foo").fetch() must_== List(Full(1L))

    Venue.where(_._id eqs v.id).select(_.geolatlng.unsafeField[Double]("lat")).fetch() must_== List(Full(40.73))

    val subuserids: List[Box[List[Long]]] = Venue.where(_._id eqs v.id).select(_.claims.subselect(_.userid)).fetch()
    subuserids must_== List(Full(List(1234, 5678)))

    // selecting a claims.userid when there is no top-level claims list should
    // have one element in the List for the one Venue, but an Empty for that
    // Venue since there's no list of claims there.
    Venue.where(_._id eqs v.id).modify(_.claims unset).and(_.lastClaim unset).updateOne()
    Venue.where(_._id eqs v.id).select(_.lastClaim.subselect(_.userid)).fetch() must_== List(Empty)
    Venue.where(_._id eqs v.id).select(_.claims.subselect(_.userid)).fetch() must_== List(Empty)
  }

  @Ignore("These tests are broken because DummyField doesn't know how to convert a String to an Enum")
  def testSelectEnumSubfield: Unit = {
    val v = baseTestVenue().save

    // This behavior is broken because we get a String back from mongo, and at
    // that point we only have a DummyField for the subfield, and that doesn't
    // know how to convert the String to an Enum.

    val statuses: List[Box[VenueClaimBson.status.MyType]] =
          Venue.where(_._id eqs v.id).select(_.lastClaim.subselect(_.status)) .fetch()
    // This assertion works.
    statuses must_== List(Full("Approved"))
    // This assertion is what we want, and it fails.
    // statuses must_== List(Full(ClaimStatus.approved))

    val subuseridsAndStatuses: List[(Box[List[Long]], Box[List[VenueClaimBson.status.MyType]])] =
          Venue.where(_._id eqs v.id)
               .select(_.claims.subselect(_.userid), _.claims.subselect(_.status))
               .fetch()
    // This assertion works.
    subuseridsAndStatuses must_== List((Full(List(1234, 5678)), Full(List("Pending approval", "Approved"))))

    // This assertion is what we want, and it fails.
    // subuseridsAndStatuses must_== List((Full(List(1234, 5678)), Full(List(ClaimStatus.pending, ClaimStatus.approved))))
  }

  @Test
  def testReadPreference: Unit = {
    // Note: this isn't a real test of readpreference because the test mongo setup
    // doesn't have replicas. This basically just makes sure that readpreference
    // doesn't break everything.
    val v = baseTestVenue().save

    // eqs
    Venue.where(_._id eqs v.id).fetch().map(_.id)                         must_== List(v.id)
    Venue.where(_._id eqs v.id).setReadPreference(ReadPreference.SECONDARY).fetch().map(_.id)        must_== List(v.id)
    Venue.where(_._id eqs v.id).setReadPreference(ReadPreference.PRIMARY).fetch().map(_.id)       must_== List(v.id)
  }

  @Test
  def testFindAndModify {
    val v1 = Venue.where(_.venuename eqs "v1")
        .findAndModify(_.userid setTo 5)
        .upsertOne(returnNew = false)
    v1 must_== None

    val v2 = Venue.where(_.venuename eqs "v2")
        .findAndModify(_.userid setTo 5)
        .upsertOne(returnNew = true)
    v2.map(_.userid.value) must_== Some(5)

    val v3 = Venue.where(_.venuename eqs "v2")
        .findAndModify(_.userid setTo 6)
        .upsertOne(returnNew = false)
    v3.map(_.userid.value) must_== Some(5)

    val v4 = Venue.where(_.venuename eqs "v2")
        .findAndModify(_.userid setTo 7)
        .upsertOne(returnNew = true)
    v4.map(_.userid.value) must_== Some(7)
  }

  @Test
  def testRegexQuery {
    val v = baseTestVenue().save
    Venue.where(_._id eqs v.id).and(_.venuename startsWith "test v").count must_== 1
    Venue.where(_._id eqs v.id).and(_.venuename matches ".es. v".r).count must_== 1
    Venue.where(_._id eqs v.id).and(_.venuename matches "Tes. v".r).count must_== 0
    Venue.where(_._id eqs v.id).and(_.venuename matches Pattern.compile("Tes. v", Pattern.CASE_INSENSITIVE)).count must_== 1
    Venue.where(_._id eqs v.id).and(_.venuename matches "test .*".r).and(_.legacyid in List(v.legacyid.value)).count must_== 1
    Venue.where(_._id eqs v.id).and(_.venuename matches "test .*".r).and(_.legacyid nin List(v.legacyid.value)).count must_== 0
  }

  @Test
  def testIteratees {
    // Insert some data
    val vs = for (i <- 1 to 10) yield {
      baseTestVenue().legacyid(i).save
    }
    val ids = vs.map(_.id)

    val items1 = Venue.where(_._id in ids)
        .iterate[List[Venue]](Nil){ case (accum, event) => {
      if (accum.length >= 3) {
        Return(accum)
      } else {
        event match {
          case Item(i) if i.legacyid.value % 2 == 0 => Continue(i :: accum)
          case Item(_) => Continue(accum)
          case EOF => Return(accum)
          case Error(e) => Return(accum)
        }
      }
    }}

    items1.map(_.legacyid.value) must_== List(6, 4, 2)

    val items2 = Venue.where(_._id in ids)
        .iterateBatch[List[Venue]](2, Nil){ case (accum, event) => {
      if (accum.length >= 3) {
        Return(accum)
      } else {
        event match {
          case Item(items) => {
            Continue(accum ++ items.filter(_.legacyid.value % 3 == 1))
          }
          case EOF => Return(accum)
          case Error(e) => Return(accum)
        }
      }
    }}

    items2.map(_.legacyid.value) must_== List(1, 4, 7)

    def findIndexOfWithLimit(id: Long, limit: Int) = {
      Venue.where(_._id in ids).iterate(1){ case (idx, event) => {
        if (idx >= limit) {
          Return(-1)
        } else {
          event match {
            case Item(i) if i.legacyid.value == id => Return(idx)
            case Item(i) => Continue(idx+1)
            case EOF => Return(-2)
            case Error(e) => Return(-3)
          }
        }
      }}
    }

    findIndexOfWithLimit(5, 2) must_== -1
    findIndexOfWithLimit(5, 7) must_== 5
    findIndexOfWithLimit(11, 12) must_== -2
  }
}
