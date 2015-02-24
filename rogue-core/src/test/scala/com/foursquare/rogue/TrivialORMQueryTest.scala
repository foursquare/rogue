package com.foursquare.rogue

import com.foursquare.index.UntypedMongoIndex
import com.foursquare.field.{Field, OptionalField}
import com.mongodb.{DB, DBCollection, DBObject, Mongo, ServerAddress, WriteConcern}
import com.foursquare.rogue.MongoHelpers.{AndCondition, MongoModify, MongoSelect}
import org.junit.{Before, Test}
import org.specs2.matcher.JUnitMustMatchers

/** A trivial ORM layer that implements the interfaces rogue needs. The goal is
  * to make sure that rogue-core works without the assistance of rogue-lift.
  * Ideally this would be even smaller; as it is, I needed to copy-paste some
  * code from the Lift implementations. */
object TrivialORM {
  trait Meta[R] {
    def collectionName: String
    def fromDBObject(dbo: DBObject): R
  }

  val mongo = {
    val MongoPort = Option(System.getenv("MONGO_PORT")).map(_.toInt).getOrElse(37648)
    new Mongo(new ServerAddress("localhost", MongoPort))
  }

  def disconnectFromMongo = {
    mongo.close
  }

  type MB = Meta[_]
  class MyDBCollectionFactory(db: DB) extends DBCollectionFactory[MB] {
    override def getDBCollection[M <: MB](query: Query[M, _, _]): DBCollection = {
      db.getCollection(query.meta.collectionName)
    }
    override def getPrimaryDBCollection[M <: MB](query: Query[M, _, _]): DBCollection = {
      db.getCollection(query.meta.collectionName)
    }
    override def getInstanceName[M <: MB](query: Query[M, _, _]): String = {
      db.getName
    }
    override def getIndexes[M <: MB](query: Query[M, _, _]): Option[List[UntypedMongoIndex]] = {
      None
    }
  }

  class MyQueryExecutor extends QueryExecutor[Meta[_]] {
    override val adapter = new MongoJavaDriverAdapter[Meta[_]](new MyDBCollectionFactory(mongo.getDB("test")))
    override val optimizer = new QueryOptimizer
    override val defaultWriteConcern: WriteConcern = WriteConcern.SAFE

    protected def serializer[M <: Meta[_], R](
      meta: M,
      select: Option[MongoSelect[M, R]]
    ): RogueSerializer[R] = new RogueSerializer[R] {
      override def fromDBObject(dbo: DBObject): R = select match {
        case Some(MongoSelect(Nil, transformer, true, _)) =>
          // A MongoSelect clause exists, but has empty fields. Return null.
          // This is used for .exists(), where we just want to check the number
          // of returned results is > 0.
          transformer(null)

        case Some(MongoSelect(fields, transformer, _, _)) =>
          transformer(fields.map(f => f.valueOrDefault(Option(dbo.get(f.field.name)))))

        case None =>
          meta.fromDBObject(dbo).asInstanceOf[R]
      }
    }
  }

  object Implicits extends Rogue {
    implicit def meta2Query[M <: Meta[R], R](meta: M with Meta[R]): Query[M, R, InitialState] = {
      Query[M, R, InitialState](
        meta, meta.collectionName, None, None, None, None, None, AndCondition(Nil, None, None), None, None, None)
    }
  }
}

case class SimpleRecord(a: Int, b: String)

object SimpleRecord extends TrivialORM.Meta[SimpleRecord] {
  val a = new OptionalField[Int, SimpleRecord.type] { override val owner = SimpleRecord; override val name = "a" }
  val b = new OptionalField[String, SimpleRecord.type] { override val owner = SimpleRecord; override val name = "b" }

  override val collectionName = "simple_records"
  override def fromDBObject(dbo: DBObject): SimpleRecord = {
    new SimpleRecord(dbo.get(a.name).asInstanceOf[Int], dbo.get(b.name).asInstanceOf[String])
  }
}

// TODO(nsanch): Everything in the rogue-lift tests should move here, except for the lift-specific extensions.
class TrivialORMQueryTest extends JUnitMustMatchers {
  val executor = new TrivialORM.MyQueryExecutor

  import TrivialORM.Implicits._

  @Before
  def cleanUpMongo = {
    executor.bulkDelete_!!(SimpleRecord)
  }

  @Test
  def canBuildQuery: Unit = {
    (SimpleRecord: Query[SimpleRecord.type, SimpleRecord, InitialState]) .toString() must_== """db.simple_records.find({ })"""
    SimpleRecord.where(_.a eqs 1)                                        .toString() must_== """db.simple_records.find({ "a" : 1})"""
  }

  @Test
  def canExecuteQuery: Unit = {
    executor.fetch(SimpleRecord.where(_.a eqs 1)) must_== Nil
    executor.count(SimpleRecord) must_== 0
  }

  @Test
  def canUpsertAndGetResults: Unit = {
    executor.count(SimpleRecord) must_== 0

    executor.upsertOne(SimpleRecord.modify(_.a setTo 1).and(_.b setTo "foo"))

    executor.count(SimpleRecord) must_== 1

    val results = executor.fetch(SimpleRecord.where(_.a eqs 1))
    results.size must_== 1
    results(0).a must_== 1
    results(0).b must_== "foo"

    executor.fetch(SimpleRecord.where(_.a eqs 1).select(_.a)) must_== List(Some(1))
    executor.fetch(SimpleRecord.where(_.a eqs 1).select(_.b)) must_== List(Some("foo"))
    executor.fetch(SimpleRecord.where(_.a eqs 1).select(_.a, _.b)) must_== List((Some(1), Some("foo")))
  }
}
