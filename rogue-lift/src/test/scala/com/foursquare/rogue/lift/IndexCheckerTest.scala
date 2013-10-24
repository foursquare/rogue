// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.lift

import com.foursquare.index.{Asc, IndexedRecord, MongoIndexChecker, TwoD}
import com.foursquare.rogue.{Degrees, LatLong, Query}
import com.foursquare.rogue.lift.LiftRogue._
import net.liftweb.mongodb.record._
import net.liftweb.mongodb.record.field._
import net.liftweb.record._
import net.liftweb.record.field.{BooleanField, IntField}
import scala.collection.immutable.ListMap

import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.junit._
import org.specs2.matcher.JUnitMustMatchers


class TestModel extends MongoRecord[TestModel] with ObjectIdKey[TestModel] {
  def meta = TestModel
  object a extends IntField(this)
  object b extends IntField(this)
  object c extends IntField(this)
  object d extends IntField(this)
  object m extends MongoMapField[TestModel, Int](this)
  object n extends MongoMapField[TestModel, Int](this)
  object ll extends MongoCaseClassField[TestModel, LatLong](this)
  object l extends MongoListField[TestModel, Int](this)
}

object TestModel extends TestModel with MongoMetaRecord[TestModel] with IndexedRecord[TestModel] {
  override def collectionName = "model"

  override val mongoIndexList = List(
    TestModel.index(_._id, Asc),
    TestModel.index(_.a, Asc, _.b, Asc, _.c, Asc),
    TestModel.index(_.m, Asc, _.a, Asc),
    TestModel.index(_.l, Asc),
    TestModel.index(_.ll, TwoD, _.b, Asc))
}

class MongoIndexCheckerTest extends JUnitMustMatchers {

  @Test
  def testIndexExpectations {
    def test(query: Query[_, _, _]) = {
      val q = query.asInstanceOf[Query[_, _, _]]
      val indexes = q.meta.asInstanceOf[IndexedRecord[_]].mongoIndexList
      MongoIndexChecker.validateIndexExpectations(q, indexes)
    }

    def yes(query: Query[_, _, _]) =
      test(query) must beTrue
    def no(query: Query[_, _, _]) =
      test(query) must beFalse

    yes(TestModel where (_.a eqs 1))
    yes(TestModel iscan (_.a eqs 1))
    yes(TestModel scan  (_.a eqs 1))

    no(TestModel where (_.a > 1))

    yes(TestModel iscan (_.a > 1))
    yes(TestModel scan  (_.a > 1))

    no(TestModel where  (_.a neqs 1))
    yes(TestModel iscan (_.a neqs 1))
    yes(TestModel scan  (_.a neqs 1))

    no(TestModel where  (_.a exists true))
    yes(TestModel iscan (_.a exists true))
    yes(TestModel scan  (_.a exists true))

    no(TestModel where (_.l size 1))
    no(TestModel iscan (_.l size 1))
    yes(TestModel scan (_.l size 1))

    no(TestModel where  (_.ll near (1.0, 2.0, Degrees(1.0))))
    yes(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))))
    yes(TestModel scan  (_.ll near (1.0, 2.0, Degrees(1.0))))

    // $or queries
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)))
    no(TestModel where (_.a eqs 1) or (_.where(_.b > 2), _.where(_.b < 2)))
    yes(TestModel where (_.a eqs 1) or (_.iscan(_.b > 2), _.iscan(_.b < 2)))
    yes(TestModel where (_.a eqs 1) or (_.scan(_.b > 2), _.scan(_.b < 2)))
    no(TestModel where (_.a eqs 1) or (_.where(_.b exists true), _.where(_.b eqs 0)))
    yes(TestModel where (_.a eqs 1) or (_.iscan(_.b exists true), _.where(_.b eqs 0)))
    yes(TestModel where (_.a eqs 1) or (_.scan(_.b exists true), _.where(_.b eqs 0)))
    no(TestModel where (_.a eqs 1) or (_.where(_.l size 1), _.where(_.b eqs 0)))
    no(TestModel where (_.a eqs 1) or (_.iscan(_.l size 1), _.where(_.b eqs 0)))
    yes(TestModel where (_.a eqs 1) or (_.scan(_.l size 1), _.where(_.b eqs 0)))
  }

  @Test
  def testMatchesIndex {
    def test(query: Query[_, _, _]) = {
      val q = query.asInstanceOf[Query[_, _, _]]
      val indexes = q.meta.asInstanceOf[IndexedRecord[_]].mongoIndexList
      MongoIndexChecker.validateIndexExpectations(q, indexes) &&
        MongoIndexChecker.validateQueryMatchesSomeIndex(q, indexes)
    }

    def yes(query: Query[_, _, _]) =
      test(query) must beTrue

    def no(query: Query[_, _, _]) =
      test(query) must beFalse

    yes(TestModel where (_.a eqs 1))
    yes(TestModel where (_.a eqs 1) and (_.b eqs 2))
    yes(TestModel where (_.a eqs 1) and (_.b eqs 2) and (_.c eqs 3))
    yes(TestModel where (_.a eqs 1) and (_.c eqs 3) and (_.b eqs 2))

    // Skip level
    yes(TestModel where (_.a eqs 1) iscan (_.c eqs 3))
    yes(TestModel where (_.a eqs 1) scan (_.c eqs 3))
    no(TestModel where (_.a eqs 1) and (_.c eqs 3))

    // Missing initial
    yes(TestModel scan (_.b eqs 2))
    no(TestModel where (_.b eqs 2))
    no(TestModel iscan (_.b eqs 2))

    // Range
    yes(TestModel iscan (_.a > 1) iscan (_.b eqs 2))
    yes(TestModel scan (_.a > 1) scan (_.b eqs 2))
    no(TestModel where (_.a > 1) and (_.b eqs 2))
    no(TestModel where (_.a > 1) iscan (_.b eqs 2))
    yes(TestModel where (_.a eqs 1) and (_.b eqs 2) iscan (_.c > 3))

    // Range placement
    yes(TestModel where (_.a eqs 1) iscan (_.b eqs 2) iscan (_.c > 3))
    yes(TestModel where (_.a eqs 1) iscan (_.b > 2) iscan (_.c eqs 3))
    yes(TestModel iscan (_.a > 1) iscan (_.b eqs 2) iscan (_.c eqs 3))

    // Double range
    yes(TestModel iscan (_.a > 1) iscan (_.b > 2) iscan (_.c eqs 3))
    no(TestModel where (_.a > 1) and (_.b > 2) and (_.c eqs 3))
    no(TestModel where (_.a > 1) and (_.b > 2) iscan (_.c eqs 3))
    no(TestModel where (_.a > 1) iscan (_.b > 2) iscan (_.c eqs 3))
    no(TestModel where (_.a > 1) iscan (_.b > 2) and (_.c eqs 3))
    yes(TestModel iscan (_.a > 1) scan (_.b > 2) scan (_.c eqs 3))
    yes(TestModel iscan (_.a > 1) scan (_.b > 2) iscan (_.c eqs 3))
    yes(TestModel where (_.a eqs 1) iscan (_.b > 2) iscan (_.c > 3))
    no(TestModel where (_.a eqs 1) and (_.b > 2) iscan (_.c > 3))

    // Index scan only
    yes(TestModel scan (_.a exists true))
    no(TestModel where (_.a exists true))
    yes(TestModel iscan (_.a exists true))

    yes(TestModel scan (_.a exists true) scan (_.b eqs 3))
    no(TestModel scan (_.a exists true) iscan (_.b eqs 3))

    // Unindexable
    yes(TestModel scan (_.l size 1))
    no(TestModel where (_.l size 1))
    no(TestModel iscan (_.l size 1))

    // Not in index
    yes(TestModel where (_.a eqs 1) scan (_.d eqs 4))
    no(TestModel where (_.a eqs 1) and (_.d eqs 4))
    no(TestModel where (_.a eqs 1) iscan (_.d eqs 4))

    // 2d indexes
    yes(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.b eqs 2))
    no(TestModel where (_.ll near (1.0, 2.0, Degrees(1.0))) and (_.b eqs 2))
    no(TestModel where (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.b eqs 2))
    no(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) and (_.b eqs 2))
    yes(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) scan (_.c eqs 2))
    no(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.c eqs 2))

    // Overspecifed queries
    val id = new ObjectId
    val d = new DateTime
    yes(TestModel where (_._id eqs id) and (_.d eqs 4))
    yes(TestModel where (_._id in List(id)) and (_.d eqs 4))
    no(TestModel where (_._id after d) scan (_.d eqs 4))
    no(TestModel iscan (_._id after d) iscan (_.d eqs 4))
    yes(TestModel iscan (_._id after d) scan (_.d eqs 4))

    // Multikeys
    yes(TestModel scan (_.m at "foo" eqs 2))
    no(TestModel where (_.m at "foo" eqs 2))
    no(TestModel iscan (_.m at "foo" eqs 2))

//TODO(markcc)    yes(TestModel where (_.n at "foo" eqs 2))
    no(TestModel where (_.n at "fo" eqs 2))
    no(TestModel where (_.n at "foot" eqs 2))
    no(TestModel where (_.n at "bar" eqs 2))

    // $or queries
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)))
    no(TestModel where (_.a eqs 1) or (_.where(_.d eqs 4), _.where(_.d eqs 4)))
    no(TestModel where (_.a eqs 1) or (_.iscan(_.d eqs 4), _.iscan(_.d eqs 4)))
    yes(TestModel where (_.a eqs 1) or (_.scan(_.d eqs 4), _.scan(_.d eqs 4)))
    no(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.d eqs 4)))
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.scan(_.d eqs 4)))
    no(TestModel where (_.a eqs 1) or (_.where(_.c eqs 3), _.where(_.c eqs 3)))
    yes(TestModel where (_.a eqs 1) or (_.iscan(_.c eqs 3), _.iscan(_.c eqs 3)))
    no(TestModel where (_.a eqs 1) and (_.b between (2, 2)) or (_.where(_.c eqs 3), _.where(_.c eqs 3)))
    yes(TestModel where (_.a eqs 1) iscan (_.b between (2, 2)) or (_.iscan(_.c eqs 3), _.iscan(_.c eqs 3)))
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)) and (_.c eqs 3))
    no(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b > 2)) and (_.c eqs 3))
    no(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.iscan(_.b > 2)) and (_.c eqs 3))
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.iscan(_.b > 2)) iscan (_.c eqs 3))
  }
}
