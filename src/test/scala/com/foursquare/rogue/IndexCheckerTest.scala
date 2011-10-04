// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.Rogue._
import net.liftweb.mongodb.record._
import net.liftweb.mongodb.record.field._
import net.liftweb.record._
import net.liftweb.record.field.{BooleanField, IntField}
import scala.collection.immutable.ListMap

import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.junit._
import org.specs.SpecsMatchers


class TestModel extends MongoRecord[TestModel] with MongoId[TestModel] {
  def meta = TestModel
  object a extends IntField(this)
  object b extends IntField(this)
  object c extends IntField(this)
  object d extends IntField(this)
  object m extends MongoMapField[TestModel, Int](this)
  object n extends MongoMapField[TestModel, Int](this)
  object ll extends MongoCaseClassField[TestModel, LatLong](this)
}

object TestModel extends TestModel with MongoMetaRecord[TestModel] {
  override def collectionName = "model"
  //  override def mongoIdentifier = NamedMongoIdentifier.venue
  val mongoIndexes = List(
    TestModel.index(_._id, Asc),
In    TestModel.index(_.a, Asc, _.b, Asc, _.c, Asc),
    TestModel.index(_.m, Asc, _.a, Asc),
    TestModel.index(_.ll, TwoD, _.b, Asc))
}

class MongoIndexCheckerTest extends SpecsMatchers with MongoQueryTypes {

  @Test
  def testIndexExpectations {
    def test(query: GenericQuery[_, _], index: List[MongoIndex[_]]) = {
      val q = query.asInstanceOf[GenericBaseQuery[_, _]]
      val clauses = MongoQueryNormalizer.normalizeCondition(q.condition)
      MongoIndexChecker.validateIndexExpectations(q, clauses)
    }

    def yes(query: GenericQuery[_, _], indexes: List[MongoIndex[_]]) =
      test(query, indexes) must beTrue
    def no(query: GenericQuery[_, _], indexes: List[MongoIndex[_]]) =
      test(query, indexes) must beFalse

    yes((TestModel where (_.a eqs 1)), TestModel.mongoIndexes)
    yes(TestModel iscan (_.a eqs 1), TestModel.mongoIndexes)
    yes(TestModel scan  (_.a eqs 1), TestModel.mongoIndexes)

    no(TestModel where (_.a > 1), TestModel.mongoIndexes)

    yes(TestModel iscan (_.a > 1), TestModel.mongoIndexes)
    yes(TestModel scan  (_.a > 1), TestModel.mongoIndexes)

    no(TestModel where (_.a neqs 1), TestModel.mongoIndexes)
    yes(TestModel iscan (_.a neqs 1), TestModel.mongoIndexes)
    yes(TestModel scan  (_.a neqs 1), TestModel.mongoIndexes)

    no(TestModel where (_.a exists true), TestModel.mongoIndexes)
    no(TestModel iscan (_.a exists true), TestModel.mongoIndexes)
    yes(TestModel scan  (_.a exists true), TestModel.mongoIndexes)

    no(TestModel where (_.ll near (1.0, 2.0, Degrees(1.0))), TestModel.mongoIndexes)
    yes(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))), TestModel.mongoIndexes)
    yes(TestModel scan (_.ll near (1.0, 2.0, Degrees(1.0))), TestModel.mongoIndexes)

    // $or queries
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.b > 2), _.where(_.b < 2)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.iscan(_.b > 2), _.iscan(_.b < 2)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.scan(_.b > 2), _.scan(_.b < 2)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.b exists true), _.where(_.b eqs 0)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.iscan(_.b exists true), _.where(_.b eqs 0)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.scan(_.b exists true), _.where(_.b eqs 0)), TestModel.mongoIndexes)
  }

  @Test
  def testMatchesIndex {
    def test(query: GenericQuery[_, _], indexes: List[MongoIndex[_]]): Boolean = {
      val q = query.asInstanceOf[GenericBaseQuery[_, _]]
      val clauses = MongoQueryNormalizer.normalizeCondition(q.condition)
      MongoIndexChecker.validateIndexExpectations(q, clauses) &&
        MongoIndexChecker.validateQueryMatchesSomeIndex(q, indexes, clauses)
    }

    def yes(query: GenericQuery[_, _], indexes: List[MongoIndex[_]]) =
      test(query, indexes) must beTrue
    def no(query: GenericQuery[_, _], indexes: List[MongoIndex[_]]) =
      test(query, indexes) must beFalse

    yes(TestModel where (_.a eqs 1), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) and (_.b eqs 2), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) and (_.b eqs 2) and (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) and (_.c eqs 3) and (_.b eqs 2), TestModel.mongoIndexes)

    // Skip level
    yes(TestModel where (_.a eqs 1) iscan (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) scan (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) and (_.c eqs 3), TestModel.mongoIndexes)

    // Missing initial
    yes(TestModel scan (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel iscan (_.b eqs 2), TestModel.mongoIndexes)

    // Range
    yes(TestModel iscan (_.a > 1) iscan (_.b eqs 2), TestModel.mongoIndexes)
    yes(TestModel scan (_.a > 1) scan (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.a > 1) and (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.a > 1) iscan (_.b eqs 2), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) and (_.b eqs 2) iscan (_.c > 3), TestModel.mongoIndexes)

    // Range placement
    yes(TestModel where (_.a eqs 1) iscan (_.b eqs 2) iscan (_.c > 3), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) iscan (_.b > 2) iscan (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel iscan (_.a > 1) iscan (_.b eqs 2) iscan (_.c eqs 3), TestModel.mongoIndexes)

    // Double range
    yes(TestModel iscan (_.a > 1) iscan (_.b > 2) iscan (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a > 1) and (_.b > 2) and (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a > 1) and (_.b > 2) iscan (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a > 1) iscan (_.b > 2) iscan (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a > 1) iscan (_.b > 2) and (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel iscan (_.a > 1) scan (_.b > 2) scan (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel iscan (_.a > 1) scan (_.b > 2) iscan (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) iscan (_.b > 2) iscan (_.c > 3), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) and (_.b > 2) iscan (_.c > 3), TestModel.mongoIndexes)

    // Unindexable
    yes(TestModel scan (_.a exists true), TestModel.mongoIndexes)
    no(TestModel where (_.a exists true), TestModel.mongoIndexes)
    no(TestModel iscan (_.a exists true), TestModel.mongoIndexes)

    yes(TestModel scan (_.a exists true) scan (_.b eqs 3), TestModel.mongoIndexes)
    no(TestModel scan (_.a exists true) iscan (_.b eqs 3), TestModel.mongoIndexes)

    // Not in index
    yes(TestModel where (_.a eqs 1) scan (_.d eqs 4), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) and (_.d eqs 4), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) iscan (_.d eqs 4), TestModel.mongoIndexes)

    // 2d indexes
    yes(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.ll near (1.0, 2.0, Degrees(1.0))) and (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.b eqs 2), TestModel.mongoIndexes)
    no(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) and (_.b eqs 2), TestModel.mongoIndexes)
    yes(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) scan (_.c eqs 2), TestModel.mongoIndexes)
    no(TestModel iscan (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.c eqs 2), TestModel.mongoIndexes)

    // Overspecifed queries
    val id = new ObjectId
    val d = new DateTime
    yes(TestModel where (_._id eqs id) and (_.d eqs 4), TestModel.mongoIndexes)
    yes(TestModel where (_._id in List(id)) and (_.d eqs 4), TestModel.mongoIndexes)
    no(TestModel where (_._id after d) scan (_.d eqs 4), TestModel.mongoIndexes)
    no(TestModel iscan (_._id after d) iscan (_.d eqs 4), TestModel.mongoIndexes)
    yes(TestModel iscan (_._id after d) scan (_.d eqs 4), TestModel.mongoIndexes)

    // Multikeys
    yes(TestModel scan (_.m at "foo" eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.m at "foo" eqs 2), TestModel.mongoIndexes)
    no(TestModel iscan (_.m at "foo" eqs 2), TestModel.mongoIndexes)

//TODO(markcc)    yes(TestModel where (_.n at "foo" eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.n at "fo" eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.n at "foot" eqs 2), TestModel.mongoIndexes)
    no(TestModel where (_.n at "bar" eqs 2), TestModel.mongoIndexes)

    // $or queries
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.d eqs 4), _.where(_.d eqs 4)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.iscan(_.d eqs 4), _.iscan(_.d eqs 4)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.scan(_.d eqs 4), _.scan(_.d eqs 4)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.d eqs 4)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.scan(_.d eqs 4)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.c eqs 3), _.where(_.c eqs 3)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.iscan(_.c eqs 3), _.iscan(_.c eqs 3)), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) and (_.b between (2, 2)) or (_.where(_.c eqs 3), _.where(_.c eqs 3)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) iscan (_.b between (2, 2)) or (_.iscan(_.c eqs 3), _.iscan(_.c eqs 3)), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)) and (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b > 2)) and (_.c eqs 3), TestModel.mongoIndexes)
    no(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.iscan(_.b > 2)) and (_.c eqs 3), TestModel.mongoIndexes)
    yes(TestModel where (_.a eqs 1) or (_.where(_.b eqs 2), _.iscan(_.b > 2)) iscan (_.c eqs 3), TestModel.mongoIndexes)
  }
}
