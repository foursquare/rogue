// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue.spindle

import com.foursquare.rogue.spindle.gen.{ThriftClaimStatus, ThriftLike, ThriftTip, ThriftVenue, ThriftVenueClaim,
    ThriftVenueClaimBson, ThriftVenueStatus}
import com.foursquare.rogue.spindle.gen.IdsTypedefs.{LikeId, TipId, VenueClaimId, VenueId}
import com.foursquare.rogue.spindle.SpindleRogue._
import com.foursquare.rogue.Iter
import com.mongodb.ReadPreference
import java.util.regex.Pattern
import org.bson.types.ObjectId
import org.junit.{After, Before, Ignore, Test}
import org.specs2.matcher.JUnitMustMatchers

/**
 * Contains tests that test the interaction of Rogue with a real mongo.
 */
class EndToEndTest extends JUnitMustMatchers {
  val db = new TestDatabaseService
  val Q = SpindleQuery

  def baseTestVenue(): ThriftVenue = {
    val claim1 = ThriftVenueClaimBson.newBuilder.userid(1234).status(ThriftClaimStatus.pending).result()
    val claim2 = ThriftVenueClaimBson.newBuilder.userid(5678).status(ThriftClaimStatus.approved).result()

    ThriftVenue.newBuilder
      .id(VenueId(new ObjectId)) // Note that this wasn't required in the lift model
      .legacyid(123)
      .userid(456)
      .venuename("test venue")
      .mayor(789)
      .mayor_count(3)
      .closed(false)
      .popularity(List(1, 2, 3))
      .categories(List(new ObjectId()))
      .geolatlng(List(40.73, -73.98))
      .status(ThriftVenueStatus.open)
      .claims(List(claim1, claim2))
      .lastClaim(claim2)
      .result()
  }
  def baseTestVenueClaim(vid: ObjectId): ThriftVenueClaim = {
    ThriftVenueClaim.newBuilder
      .id(VenueClaimId(new ObjectId)) // Note that this wasn't required in the lift model
      .venueId(VenueId(vid))
      .userId(123)
      .status(ThriftClaimStatus.approved)
      .result()
  }

  def baseTestTip(): ThriftTip = {
    ThriftTip.newBuilder
      .id(TipId(new ObjectId)) // Note that this wasn't required in the lift model
      .legacyid(234)
      .counts(Map("foo" -> 1, "bar" -> 2))
      .result()
  }

  @After
  def cleanupTestData: Unit = {
    db.bulkDelete_!!(Q(ThriftVenue))
    db.count(Q(ThriftVenue)) must_== 0

    db.bulkDelete_!!(Q(ThriftVenueClaim))
    db.count(Q(ThriftVenueClaim)) must_== 0

    db.bulkDelete_!!(Q(ThriftLike)) //TODO(rogue-shards): should have shards and be an allShards.bulkDelete_!!!
    db.count(Q(ThriftLike)) must_== 0

    db.disconnectFromMongo
  }

  @Test
  def testInsertAll {
    val v1 = baseTestVenue
    val vs = List(baseTestVenue, v1, baseTestVenue, baseTestVenue)
    db.insert(v1)
    try {
      db.insertAll(vs)
    } catch {
      case e =>
    }
    db.count(Q(ThriftVenue)) must_== 2
  }

  @Test
  def eqsTests: Unit = {
    val v = baseTestVenue()
    db.insert(v)

    val vc = baseTestVenueClaim(v.id)
    db.insert(vc)

    db.fetch(Q(ThriftVenue).where(_.id eqs v.id)).map(_.id)                                  must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor eqs v.mayor)).map(_.id)                            must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor eqs v.mayor)).map(_.id)                            must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.venuename eqs v.venuenameOrThrow)).map(_.id)             must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.closed eqs false)).map(_.id)                             must_== Vector(v.id)

    db.fetch(Q(ThriftVenue).where(_.mayor eqs 432432)).map(_.id)                             must_== Nil
    db.fetch(Q(ThriftVenue).where(_.closed eqs true)).map(_.id)                              must_== Nil

    db.fetch(Q(ThriftVenueClaim).where(_.status eqs ThriftClaimStatus.approved)).map(_.id)   must_== Vector(vc.id)
    db.fetch(Q(ThriftVenueClaim).where(_.venueId eqs v.id)).map(_.id)                        must_== Vector(vc.id)
    //TODO(rogue-foreign-key-support)
    //db.fetch(Q(ThriftVenueClaim).where(_.venueId eqs v)).map(_.id)                         must_== Vector(vc.id)
  }

  @Test
  def testInequalityQueries: Unit = {
    val v = db.insert(baseTestVenue())
    val vc = db.insert(baseTestVenueClaim(v.id))

    // neq,lt,gt, where the lone Venue has mayor_count=3, and the only
    // VenueClaim has status approved.
    db.fetch(Q(ThriftVenue).where(_.mayor_count neqs 5)).map(_.id)                     must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor_count < 5)).map(_.id)                        must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor_count lt 5)).map(_.id)                       must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor_count <= 5)).map(_.id)                       must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor_count lte 5)).map(_.id)                      must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.mayor_count > 5)).map(_.id)                        must_== Nil
    db.fetch(Q(ThriftVenue).where(_.mayor_count gt 5)).map(_.id)                       must_== Nil
    db.fetch(Q(ThriftVenue).where(_.mayor_count >= 5)).map(_.id)                       must_== Nil
    db.fetch(Q(ThriftVenue).where(_.mayor_count gte 5)).map(_.id)                      must_== Nil
    db.fetch(Q(ThriftVenue).where(_.mayor_count between (3, 5))).map(_.id)             must_== Vector(v.id)
    db.fetch(Q(ThriftVenueClaim).where (_.status neqs ThriftClaimStatus.approved)).map(_.id) must_== Nil
    db.fetch(Q(ThriftVenueClaim).where (_.status neqs ThriftClaimStatus.pending)).map(_.id)  must_== Vector(vc.id)
  }

  @Test
  def selectQueries: Unit = {
    val v = db.insert(baseTestVenue())

    val base = Q(ThriftVenue).where(_.id eqs v.id)
    db.fetch(base.select(_.legacyid)) must_== Vector(v.legacyidOption)
    db.fetch(base.select(_.legacyid, _.userid)) must_== Vector((v.legacyidOption, v.useridOption))
    db.fetch(base.select(_.legacyid, _.userid, _.mayor)) must_== Vector((v.legacyidOption, v.useridOption, v.mayorOption))
    db.fetch(base.select(_.legacyid, _.userid, _.mayor, _.mayor_count)) must_== Vector((v.legacyidOption, v.useridOption, v.mayorOption, v.mayor_countOption))
    db.fetch(base.select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed)) must_== Vector((v.legacyidOption, v.useridOption, v.mayorOption, v.mayor_countOption, v.closedOption))
    db.fetch(base.select(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags)) must_== Vector((v.legacyidOption, v.useridOption, v.mayorOption, v.mayor_countOption, v.closedOption, v.tagsOption))
  }

  @Test
  def selectEnum: Unit = {
    val v = db.insert(baseTestVenue())
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.status)) must_== Vector(Some(ThriftVenueStatus.open))
  }

  @Test
  def selectCaseQueries: Unit = {
    val v = db.insert(baseTestVenue())

    val base = Q(ThriftVenue).where(_.id eqs v.id)
    db.fetch(base.selectCase(_.legacyid, V1)) must_== Vector(V1(v.legacyidOption))
    db.fetch(base.selectCase(_.legacyid, _.userid, V2)) must_== Vector(V2(v.legacyidOption, v.useridOption))
    db.fetch(base.selectCase(_.legacyid, _.userid, _.mayor, V3)) must_== Vector(V3(v.legacyidOption, v.useridOption, v.mayorOption))
    db.fetch(base.selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, V4)) must_== Vector(V4(v.legacyidOption, v.useridOption, v.mayorOption, v.mayor_countOption))
    db.fetch(base.selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, V5)) must_== Vector(V5(v.legacyidOption, v.useridOption, v.mayorOption, v.mayor_countOption, v.closedOption))
    db.fetch(base.selectCase(_.legacyid, _.userid, _.mayor, _.mayor_count, _.closed, _.tags, V6)) must_== Vector(V6(v.legacyidOption, v.useridOption, v.mayorOption, v.mayor_countOption, v.closedOption, v.tagsOption))
  }

  @Test
  def selectSubfieldQueries: Unit = {
    val v = db.insert(baseTestVenue())
    val t = db.insert(baseTestTip())

    // Sub-select on map
    db.fetch(Q(ThriftTip).where(_.id eqs t.id).select(_.counts at "foo")) must_== Vector(Some(1))
    // Sub-select on embedded record
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.lastClaim.sub.select(_.status))) must_== Vector(Some(ThriftClaimStatus.approved))
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.lastClaim.unsafeField[ThriftClaimStatus]("status"))) must_== Vector(Some(ThriftClaimStatus.approved))
    // Sub-select on embedded record
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.claims.sub.select(_.status))) must_== Vector(Some(Vector(Some(ThriftClaimStatus.pending), Some(ThriftClaimStatus.approved))))

    val subuserids: Seq[Option[Seq[Option[Long]]]] = db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.claims.sub.select(_.userid)))
    subuserids must_== Vector(Some(Vector(Some(1234), Some(5678))))

    val subclaims: Seq[Option[Seq[ThriftVenueClaimBson]]] = db.fetch(Q(ThriftVenue).where(_.claims.sub.field(_.userid) eqs 1234).select(_.claims.$$))
    subclaims.size must_== 1
    subclaims.head.isEmpty must_== false
    subclaims.head.get.size must_== 1
    subclaims.head.get.head.userid must_== 1234
    subclaims.head.get.head.statusOption must_== Some(ThriftClaimStatus.pending)

    // selecting a claims.userid when there is no top-level claims list should
    // have one element in the List for the one Venue, but an Empty for that
    // Venue since there's no list of claims there.
    db.updateOne(Q(ThriftVenue).where(_.id eqs v.id).modify(_.claims unset).and(_.lastClaim unset))
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.lastClaim.sub.select(_.userid))) must_== List(None)
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.claims.sub.select(_.userid))) must_== List(None)
  }

  /* TODO(rogue-named-enums) */
  @Ignore("These tests are broken because DummyField doesn't know how to convert a String to an Enum")
  def testSelectEnumSubfield: Unit = {
    val v = db.insert(baseTestVenue())

    // This behavior is broken because we get a String back from mongo, and at
    // that point we only have a DummyField for the subfield, and that doesn't
    // know how to convert the String to an Enum.

    val statuses: Seq[Option[ThriftClaimStatus]] =
      db.fetch(Q(ThriftVenue).where(_.id eqs v.id).select(_.lastClaim.sub.select(_.status)))
    // This assertion works.
    statuses must_== Vector(Some("Approved"))
    // This assertion is what we want, and it fails.
    // statuses must_== Vector(Some(ThriftClaimStatus.approved))

    /* TODO(rogue-sub-on-listfield)
    val subuseridsAndStatuses: List[(Option[List[Long]], Option[List[ThriftClaimStatus]])] =
      db.fetch(Q(ThriftVenue).where(_.id eqs v.id)
        .select(_.claims.sub.select(_.userid), _.claims.sub.select(_.status)))
    // This assertion works.
    subuseridsAndStatuses must_== List((Some(List(1234, 5678)), Some(List("Pending approval", "Approved"))))
    */

    // This assertion is what we want, and it fails.
    // subuseridsAndStatuses must_== List((Some(List(1234, 5678)), Some(List(ThriftClaimStatus.pending, ThriftClaimStatus.approved))))
  }

  @Test
  def testReadPreference: Unit = {
    // Note: this isn't a real test of readpreference because the test mongo setup
    // doesn't have replicas. This basically just makes sure that readpreference
    // doesn't break everything.
    val v = db.insert(baseTestVenue())

    // eqs
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id)).map(_.id)                                               must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).setReadPreference(ReadPreference.secondary)).map(_.id)   must_== Vector(v.id)
    db.fetch(Q(ThriftVenue).where(_.id eqs v.id).setReadPreference(ReadPreference.primary)).map(_.id)     must_== Vector(v.id)
  }

  @Test
  def testFindAndModify {
    val v1 = db.findAndUpsertOne(
      Q(ThriftVenue).where(_.venuename eqs "v1").findAndModify(_.userid setTo 5),
      returnNew = false
    )
    v1 must_== None

    val v2 = db.findAndUpsertOne(
      Q(ThriftVenue).where(_.venuename eqs "v2").findAndModify(_.userid setTo 5),
      returnNew = true
    )
    v2.map(_.userid) must_== Some(5)

    val v3 = db.findAndUpsertOne(
      Q(ThriftVenue).where(_.venuename eqs "v2").findAndModify(_.userid setTo 6),
      returnNew = false
    )
    v3.map(_.userid) must_== Some(5)

    val v4 = db.findAndUpsertOne(
      Q(ThriftVenue).where(_.venuename eqs "v2").findAndModify(_.userid setTo 7),
      returnNew = true
    )
    v4.map(_.userid) must_== Some(7)
  }

  @Test
  def testRegexQuery {
    val v = db.insert(baseTestVenue())
    db.count(Q(ThriftVenue).where(_.id eqs v.id).and(_.venuename startsWith "test v")) must_== 1
    db.count(Q(ThriftVenue).where(_.id eqs v.id).and(_.venuename matches ".es. v".r)) must_== 1
    db.count(Q(ThriftVenue).where(_.id eqs v.id).and(_.venuename matches "Tes. v".r)) must_== 0
    db.count(Q(ThriftVenue).where(_.id eqs v.id).and(_.venuename matches Pattern.compile("Tes. v", Pattern.CASE_INSENSITIVE))) must_== 1
    db.count(Q(ThriftVenue).where(_.id eqs v.id).and(_.venuename matches "test .*".r).and(_.legacyid in List(v.legacyid))) must_== 1
    db.count(Q(ThriftVenue).where(_.id eqs v.id).and(_.venuename matches "test .*".r).and(_.legacyid nin List(v.legacyid))) must_== 0
  }

  @Test
  def testIterates {
    // Insert some data
    val vs = for (i <- 1 to 10) yield {
      db.insert(baseTestVenue().toBuilder.legacyid(i).result())
    }
    val ids = vs.map(_.id)

    val empty: Seq[ThriftVenue] = Nil
    val items1 = db.iterate(Q(ThriftVenue).where(_.id in ids), empty) { case (accum, event) => {
      if (accum.length >= 3) {
        Iter.Return(accum)
      } else {
        event match {
          case Iter.Item(i) if i.legacyid % 2 == 0 => Iter.Continue(i +: accum)
          case Iter.Item(_) => Iter.Continue(accum)
          case Iter.EOF => Iter.Return(accum)
          case Iter.Error(e) => Iter.Return(accum)
        }
      }
    }}

    items1.map(_.legacyid) must_== List(6, 4, 2)

    val items2 = db.iterateBatch(Q(ThriftVenue).where(_.id in ids), 2, empty) { case (accum, event) => {
      if (accum.length >= 3) {
        Iter.Return(accum)
      } else {
        event match {
          case Iter.Item(items) => {
            Iter.Continue(accum ++ items.filter(_.legacyid % 3 == 1))
          }
          case Iter.EOF => Iter.Return(accum)
          case Iter.Error(e) => Iter.Return(accum)
        }
      }
    }}

    items2.map(_.legacyid) must_== List(1, 4, 7)

    def findIndexOfWithLimit(id: Long, limit: Int) = {
      db.iterate(Q(ThriftVenue).where(_.id in ids), 1) { case (idx, event) => {
        if (idx >= limit) {
          Iter.Return(-1)
        } else {
          event match {
            case Iter.Item(i) if i.legacyid == id => Iter.Return(idx)
            case Iter.Item(i) => Iter.Continue(idx+1)
            case Iter.EOF => Iter.Return(-2)
            case Iter.Error(e) => Iter.Return(-3)
          }
        }
      }}
    }

    findIndexOfWithLimit(5, 2) must_== -1
    findIndexOfWithLimit(5, 7) must_== 5
    findIndexOfWithLimit(11, 12) must_== -2
  }

  @Test
  def testSharding {
    val l1 = db.insert(ThriftLike.newBuilder.id(LikeId(new ObjectId)).userid(1).checkin(111).result())
    val l2 = db.insert(ThriftLike.newBuilder.id(LikeId(new ObjectId)).userid(2).checkin(111).result())
    val l3 = db.insert(ThriftLike.newBuilder.id(LikeId(new ObjectId)).userid(2).checkin(333).result())
    val l4 = db.insert(ThriftLike.newBuilder.id(LikeId(new ObjectId)).userid(2).checkin(444).result())

    // Find
    db.count(Q(ThriftLike).where(_.checkin eqs 111).allShards) must_== 2
    /* TODO(rogue-shards)
    db.count(Q(ThriftLike).where(_.checkin eqs 111).withShardKey(_.userid eqs 1)) must_== 1
    db.count(Q(ThriftLike).withShardKey(_.userid eqs 1).where(_.checkin eqs 111)) must_== 1
    db.count(Q(ThriftLike).withShardKey(_.userid eqs 1)) must_== 1
    db.count(Q(ThriftLike).withShardKey(_.userid eqs 2)) must_== 3
    db.count(Q(ThriftLike).where(_.checkin eqs 333).withShardKey(_.userid eqs 2)) must_== 1

    // Modify
    db.updateOne(Q(ThriftLike).withShardKey(_.userid eqs 2).and(_.checkin eqs 333).modify(_.checkin setTo 334))
    Q(ThriftLike).find(l3.id).openOrThrowException("automated transition from open_!").checkin must_== 334
    db.count(Q(ThriftLike).where(_.checkin eqs 334).allShards) must_== 1

    db.updateMulti(Q(ThriftLike).where(_.checkin eqs 111).allShards.modify(_.checkin setTo 112))
    db.count(Q(ThriftLike).where(_.checkin eqs 112).withShardKey(_.userid in List(1L, 2L))) must_== 2

    val l5 = findAndUpdateOne(Q(ThriftLike).where(_.checkin eqs 112).withShardKey(_.userid eqs 1).findAndModify(_.checkin setTo 113), returnNew = true)
    l5.get.id must_== l1.id
    l5.get.checkin must_== 113
    */
  }

  @Test
  def testLimitAndBatch {
    db.insertAll(List.fill(50)(baseTestVenue()))

    val q = Q(ThriftVenue).select(_.id)
    db.fetch(q.limit(10)).length must_== 10
    db.fetch(q.limit(-10)).length must_== 10
    db.fetchBatch(q, 20)(x => Vector(x.length)) must_== Vector(20, 20, 10)
    db.fetchBatch(q.limit(35), 20)(x => Vector(x.length)) must_== Vector(20, 15)
    db.fetchBatch(q.limit(-35), 20)(x => Vector(x.length)) must_== Vector(20, 15)
  }

  @Test
  def testCount {
    db.insertAll(List.fill(10)(baseTestVenue()))
    val q = Q(ThriftVenue).select(_.id)
    db.count(q) must_== 10
    db.count(q.limit(3)) must_== 3
    db.count(q.limit(15)) must_== 10
    db.count(q.skip(5)) must_== 5
    db.count(q.skip(12)) must_== 0
    db.count(q.skip(3).limit(5)) must_== 5
    db.count(q.skip(8).limit(4)) must_== 2
  }

  @Test
  def testSlice {
    db.insert(baseTestVenue().toBuilder.tags(List("1", "2", "3", "4")).result())
    /* TODO(rogue-select-option-option): This should probably not return an option[option[list]] */
    db.fetchOne(Q(ThriftVenue).select(_.tags.slice(2))) must_== Some(Some(Vector("1", "2")))
    db.fetchOne(Q(ThriftVenue).select(_.tags.slice(-2))) must_== Some(Some(Vector("3", "4")))
    db.fetchOne(Q(ThriftVenue).select(_.tags.slice(1, 2))) must_== Some(Some(Vector("2", "3")))
  }

  @Test
  def testSave {
    val v = baseTestVenue()
    db.insert(v)
    val mutable = v.mutable
    mutable.tags_=(List("a", "bbbb"))
    db.save(mutable)

    db.count(Q(ThriftVenue).where(_.tags contains "bbbb")) must_== 1
  }

  @Test
  def testDistinct {
    db.insertAll((1 to 5).map(_ => baseTestVenue().toBuilder.userid(1).result()))
    db.insertAll((1 to 5).map(_ => baseTestVenue().toBuilder.userid(2).result()))
    db.insertAll((1 to 5).map(_ => baseTestVenue().toBuilder.userid(3).result()))
    db.distinct(Q(ThriftVenue).where(_.mayor eqs 789))(_.userid).length must_== 3
    db.countDistinct(Q(ThriftVenue).where(_.mayor eqs 789))(_.userid) must_== 3

    db.insertAll((1 to 10).map(i => baseTestVenue().toBuilder.userid(222).venuename("test " + (i%3)).result()))
    val names = db.distinct(Q(ThriftVenue).where(_.userid eqs 222))(_.venuename)
    names.sorted must_== Vector("test 0", "test 1", "test 2")
  }

  @Test
  def testListMethods: Unit = {
    db.insertAll((1 to 5).map(_ => baseTestVenue().toBuilder.userid(1).result()))
    db.insertAll((1 to 5).map(_ => baseTestVenue().toBuilder.userid(2).result()))
    db.insertAll((1 to 5).map(_ => baseTestVenue().toBuilder.userid(3).result()))

    val vs: Seq[ThriftVenue] = db.fetchList(Q(ThriftVenue).where(_.mayor eqs 789))
    vs.size must_== 15

    val ids: Seq[VenueId] = db.fetchBatchList(Q(ThriftVenue).where(_.mayor eqs 789), 3)(b => b.map(_.id))
    ids.size must_== 15
  }
}
