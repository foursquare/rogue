// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.spindle

import com.foursquare.index.{IndexedRecord, MongoIndexChecker}
import com.foursquare.rogue.Query
import com.foursquare.rogue.Rogue._
import com.foursquare.rogue.spindle.gen.IdsTypedefs.IndexTestId
import com.foursquare.rogue.spindle.gen.{ThriftIndexTestModel, MutuallyRecursive1}
import com.foursquare.spindle.UntypedMetaRecord
import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.junit._
import org.specs2.matcher.JUnitMustMatchers
import scala.collection.immutable.ListMap

/**
 * Test spindle index annotations.
 */
class MongoIndexCheckerTest extends JUnitMustMatchers {
  type IndexModelType = ThriftIndexTestModel
  val db = new TestDatabaseService
  val Q = SpindleQuery

  @Test
  def testGetIndexes {
    val indexesOpt = db.dbCollectionFactory.getIndexes(Q(ThriftIndexTestModel))

    indexesOpt.map(_.map(_.asListMap)) must_== Some(List(
      ListMap("_id" -> "1"),
      ListMap("a" -> "1", "b" -> "1", "c" -> "1"),
      ListMap("m" -> "1", "a" -> "1"),
      ListMap("l" -> "1"),
      ListMap("ll" -> "2d", "b" -> "1"),
      ListMap("e.i" -> "-1")
    ))

    indexesOpt.map(_.map(_.toString)) must_== Some(List(
      "_id:1",
      "a:1, b:1, c:1",
      "m:1, a:1",
      "l:1",
      "ll:2d, b:1",
      "e.i:-1"
    ))
  }

  @Test
  def testGetIndexesWithMutuallyRecursiveStructs {
    val indexesOpt = db.dbCollectionFactory.getIndexes(Q(MutuallyRecursive1))
    indexesOpt.map(_.map(_.toString)) must_== Some(List(
      "_id:1",
      "m2.m1:1"
    ))
  }

  @Test
  def testIndexExpectations {
    def test[M <: UntypedMetaRecord](query: Query[M, _, _]) = {
      val indexesOpt = db.dbCollectionFactory.getIndexes(query)
      indexesOpt.forall(indexes =>
        MongoIndexChecker.validateIndexExpectations(query, indexes))
    }

    def yes[M <: UntypedMetaRecord](query: Query[M, _, _]) =
      test(query) must beTrue
    def no[M <: UntypedMetaRecord](query: Query[M, _, _]) =
      test(query) must beFalse

    yes(Q(ThriftIndexTestModel) where (_.a eqs 1))
    yes(Q(ThriftIndexTestModel) iscan (_.a eqs 1))
    yes(Q(ThriftIndexTestModel) scan  (_.a eqs 1))

    no(Q(ThriftIndexTestModel) where (_.a > 1))

    yes(Q(ThriftIndexTestModel) iscan (_.a > 1))
    yes(Q(ThriftIndexTestModel) scan  (_.a > 1))

    no(Q(ThriftIndexTestModel) where  (_.a neqs 1))
    yes(Q(ThriftIndexTestModel) iscan (_.a neqs 1))
    yes(Q(ThriftIndexTestModel) scan  (_.a neqs 1))

    no(Q(ThriftIndexTestModel) where  (_.a exists true))
    yes(Q(ThriftIndexTestModel) iscan (_.a exists true))
    yes(Q(ThriftIndexTestModel) scan  (_.a exists true))

    no(Q(ThriftIndexTestModel) where (_.l size 1))
    no(Q(ThriftIndexTestModel) iscan (_.l size 1))
    yes(Q(ThriftIndexTestModel) scan (_.l size 1))

    /* TODO(rogue-latlng)
    no(Q(ThriftIndexTestModel) where  (_.ll near (1.0, 2.0, Degrees(1.0))))
    yes(Q(ThriftIndexTestModel) iscan (_.ll near (1.0, 2.0, Degrees(1.0))))
    yes(Q(ThriftIndexTestModel) scan  (_.ll near (1.0, 2.0, Degrees(1.0))))
    */

    // $or queries
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b > 2), _.where(_.b < 2)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.iscan(_.b > 2), _.iscan(_.b < 2)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.scan(_.b > 2), _.scan(_.b < 2)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b exists true), _.where(_.b eqs 0)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.iscan(_.b exists true), _.where(_.b eqs 0)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.scan(_.b exists true), _.where(_.b eqs 0)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.l size 1), _.where(_.b eqs 0)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.iscan(_.l size 1), _.where(_.b eqs 0)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.scan(_.l size 1), _.where(_.b eqs 0)))
  }

  @Test
  def testMatchesIndex {
    def test[M <: UntypedMetaRecord](query: Query[M, _, _]) = {
      val q = query.asInstanceOf[Query[_, _, _]]
      val indexesOpt = db.dbCollectionFactory.getIndexes(query)
      indexesOpt.forall(indexes =>
        MongoIndexChecker.validateIndexExpectations(q, indexes) &&
          MongoIndexChecker.validateQueryMatchesSomeIndex(q, indexes))
    }

    def yes[M <: UntypedMetaRecord](query: Query[M, _, _]) =
      test(query) must beTrue

    def no[M <: UntypedMetaRecord](query: Query[M, _, _]) =
      test(query) must beFalse

    yes(Q(ThriftIndexTestModel) where (_.a eqs 1))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.b eqs 2))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.b eqs 2) and (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.c eqs 3) and (_.b eqs 2))

    // Skip level
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) iscan (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) scan (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.c eqs 3))

    // Missing initial
    yes(Q(ThriftIndexTestModel) scan (_.b eqs 2))
    no(Q(ThriftIndexTestModel) where (_.b eqs 2))
    no(Q(ThriftIndexTestModel) iscan (_.b eqs 2))

    // Range
    yes(Q(ThriftIndexTestModel) iscan (_.a > 1) iscan (_.b eqs 2))
    yes(Q(ThriftIndexTestModel) scan (_.a > 1) scan (_.b eqs 2))
    no(Q(ThriftIndexTestModel) where (_.a > 1) and (_.b eqs 2))
    no(Q(ThriftIndexTestModel) where (_.a > 1) iscan (_.b eqs 2))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.b eqs 2) iscan (_.c > 3))

    // Range placement
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) iscan (_.b eqs 2) iscan (_.c > 3))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) iscan (_.b > 2) iscan (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) iscan (_.a > 1) iscan (_.b eqs 2) iscan (_.c eqs 3))

    // Double range
    yes(Q(ThriftIndexTestModel) iscan (_.a > 1) iscan (_.b > 2) iscan (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a > 1) and (_.b > 2) and (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a > 1) and (_.b > 2) iscan (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a > 1) iscan (_.b > 2) iscan (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a > 1) iscan (_.b > 2) and (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) iscan (_.a > 1) scan (_.b > 2) scan (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) iscan (_.a > 1) scan (_.b > 2) iscan (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) iscan (_.b > 2) iscan (_.c > 3))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.b > 2) iscan (_.c > 3))

    // Index scan only
    yes(Q(ThriftIndexTestModel) scan (_.a exists true))
    no(Q(ThriftIndexTestModel) where (_.a exists true))
    yes(Q(ThriftIndexTestModel) iscan (_.a exists true))

    yes(Q(ThriftIndexTestModel) scan (_.a exists true) scan (_.b eqs 3))
    no(Q(ThriftIndexTestModel) scan (_.a exists true) iscan (_.b eqs 3))

    // Unindexable
    yes(Q(ThriftIndexTestModel) scan (_.l size 1))
    no(Q(ThriftIndexTestModel) where (_.l size 1))
    no(Q(ThriftIndexTestModel) iscan (_.l size 1))

    // Not in index
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) scan (_.d eqs 4))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.d eqs 4))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) iscan (_.d eqs 4))

    // 2d indexes
    /* TODO(rogue-latlng)
    yes(Q(ThriftIndexTestModel) iscan (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.b eqs 2))
    no(Q(ThriftIndexTestModel) where (_.ll near (1.0, 2.0, Degrees(1.0))) and (_.b eqs 2))
    no(Q(ThriftIndexTestModel) where (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.b eqs 2))
    no(Q(ThriftIndexTestModel) iscan (_.ll near (1.0, 2.0, Degrees(1.0))) and (_.b eqs 2))
    yes(Q(ThriftIndexTestModel) iscan (_.ll near (1.0, 2.0, Degrees(1.0))) scan (_.c eqs 2))
    no(Q(ThriftIndexTestModel) iscan (_.ll near (1.0, 2.0, Degrees(1.0))) iscan (_.c eqs 2))
    */

    // Overspecifed queries
    val id = new ObjectId
    val d = new DateTime
    yes(Q(ThriftIndexTestModel) where (_.id eqs IndexTestId(id)) and (_.d eqs 4))
    yes(Q(ThriftIndexTestModel) where (_.id in List(IndexTestId(id))) and (_.d eqs 4))
    no(Q(ThriftIndexTestModel) where (_.id after d) scan (_.d eqs 4))
    no(Q(ThriftIndexTestModel) iscan (_.id after d) iscan (_.d eqs 4))
    yes(Q(ThriftIndexTestModel) iscan (_.id after d) scan (_.d eqs 4))

    // Multikeys
    yes(Q(ThriftIndexTestModel) scan (_.m at "foo" eqs 2))
    no(Q(ThriftIndexTestModel) where (_.m at "foo" eqs 2))
    no(Q(ThriftIndexTestModel) iscan (_.m at "foo" eqs 2))

//TODO(markcc)    yes(Q(ThriftIndexTestModel) where (_.n at "foo" eqs 2))
    no(Q(ThriftIndexTestModel) where (_.n at "fo" eqs 2))
    no(Q(ThriftIndexTestModel) where (_.n at "foot" eqs 2))
    no(Q(ThriftIndexTestModel) where (_.n at "bar" eqs 2))

    // $or queries
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.d eqs 4), _.where(_.d eqs 4)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.iscan(_.d eqs 4), _.iscan(_.d eqs 4)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.scan(_.d eqs 4), _.scan(_.d eqs 4)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.d eqs 4)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.scan(_.d eqs 4)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.c eqs 3), _.where(_.c eqs 3)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.iscan(_.c eqs 3), _.iscan(_.c eqs 3)))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) and (_.b between (2, 2)) or (_.where(_.c eqs 3), _.where(_.c eqs 3)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) iscan (_.b between (2, 2)) or (_.iscan(_.c eqs 3), _.iscan(_.c eqs 3)))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b eqs 2)) and (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.where(_.b > 2)) and (_.c eqs 3))
    no(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.iscan(_.b > 2)) and (_.c eqs 3))
    yes(Q(ThriftIndexTestModel) where (_.a eqs 1) or (_.where(_.b eqs 2), _.iscan(_.b > 2)) iscan (_.c eqs 3))
  }
}
