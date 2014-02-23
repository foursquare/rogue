// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue.lift

import com.foursquare.field.{
    Field => RField,
    OptionalField => ROptionalField,
    RequiredField => RRequiredField}
import com.foursquare.index.IndexBuilder
import com.foursquare.rogue.{BSONType, BsonRecordListModifyField, BsonRecordListQueryField, BsonRecordModifyField,
    BsonRecordQueryField, DateModifyField,
    DateQueryField, EnumerationListModifyField, EnumerationListQueryField, EnumerationModifyField, EnumIdQueryField,
    EnumNameQueryField, FindAndModifyQuery, ForeignObjectIdQueryField, GeoModifyField, GeoQueryField, HasOrClause,
    InitialState, LatLong, ListModifyField, ListQueryField, MandatorySelectField, MapModifyField, MapQueryField,
    ModifyField, ModifyQuery, NumericModifyField, NumericQueryField, ObjectIdQueryField, OptionalSelectField, Query,
    QueryField, QueryHelpers, Rogue, RogueException, SafeModifyField, SelectField, ShardingOk, StringsListQueryField,
    StringQueryField, Unlimited, Unordered, Unselected, Unskipped}
import com.foursquare.rogue.MongoHelpers.{AndCondition, MongoModify}
import java.util.Date
import net.liftweb.json.JsonAST.{JArray, JInt}
import net.liftweb.common.Box.box2Option
import net.liftweb.mongodb.record.{BsonRecord, MongoRecord, MongoMetaRecord}
import net.liftweb.record.{Field, MandatoryTypedField, OptionalTypedField, Record}
import net.liftweb.mongodb.record.field.{ BsonRecordField, BsonRecordListField, MongoCaseClassField,
  MongoCaseClassListField}
import org.bson.types.ObjectId
import net.liftweb.record.field.EnumField

trait LiftRogue {
  def OrQuery[M <: MongoRecord[M], R]
      (subqueries: Query[M, R, _]*)
      : Query[M, R, Unordered with Unselected with Unlimited with Unskipped with HasOrClause] = {
    subqueries.toList match {
      case Nil => throw new RogueException("No subqueries supplied to OrQuery", null)
      case q :: qs => {
        val orCondition = QueryHelpers.orConditionFromQueries(q :: qs)
        Query[M, R, Unordered with Unselected with Unlimited with Unskipped with HasOrClause](
          q.meta, q.collectionName, None, None, None, None, None,
          AndCondition(Nil, Some(orCondition)), None, None, None)
      }
    }
  }

  /* Following are a collection of implicit conversions which take a meta-record and convert it to
   * a QueryBuilder. This allows users to write queries as "QueryType where ...".
   */
  implicit def metaRecordToQueryBuilder[M <: MongoRecord[M]]
      (rec: M with MongoMetaRecord[M]): Query[M, M, InitialState] =
    Query[M, M, InitialState](
      rec, rec.collectionName, None, None, None, None, None, AndCondition(Nil, None), None, None, None)

  implicit def metaRecordToIndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): IndexBuilder[M] =
      IndexBuilder(rec)

  implicit def queryToLiftQuery[M <: MongoRecord[_], R, State]
    (query: Query[M, R, State])
    (implicit ev: ShardingOk[M with MongoMetaRecord[_], State]): ExecutableQuery[MongoRecord[_] with MongoMetaRecord[_], M with MongoMetaRecord[_], MongoRecord[_], R, State] = {
    ExecutableQuery(
        query.asInstanceOf[Query[M with MongoMetaRecord[_], R, State]],
        LiftQueryExecutor
    )
  }

  implicit def modifyQueryToLiftModifyQuery[M <: MongoRecord[_], State](
      query: ModifyQuery[M, State]
  ): ExecutableModifyQuery[MongoRecord[_] with MongoMetaRecord[_], M with MongoMetaRecord[_], MongoRecord[_], State] = {
    ExecutableModifyQuery(
        query.asInstanceOf[ModifyQuery[M with MongoMetaRecord[_], State]],
        LiftQueryExecutor
    )
  }

  implicit def findAndModifyQueryToLiftFindAndModifyQuery[M <: MongoRecord[_], R](
      query: FindAndModifyQuery[M, R]
  ): ExecutableFindAndModifyQuery[MongoRecord[_] with MongoMetaRecord[_], M with MongoMetaRecord[_], MongoRecord[_], R] = {
    ExecutableFindAndModifyQuery(
        query.asInstanceOf[FindAndModifyQuery[M with MongoMetaRecord[_], R]],
        LiftQueryExecutor
    )
  }

  implicit def metaRecordToLiftQuery[M <: MongoRecord[M]](
      rec: M with MongoMetaRecord[M]
  ): ExecutableQuery[MongoRecord[_] with MongoMetaRecord[_], M with MongoMetaRecord[_], MongoRecord[_], M, InitialState] = {
    val queryBuilder = metaRecordToQueryBuilder(rec)
    val liftQuery = queryToLiftQuery(queryBuilder)
    liftQuery
  }

  implicit def fieldToQueryField[M <: BsonRecord[M], F: BSONType](f: Field[F, M]): QueryField[F, M] = new QueryField(f)

  implicit def bsonRecordFieldToBsonRecordQueryField[
      M <: BsonRecord[M],
      B <: BsonRecord[B]
  ](
      f: BsonRecordField[M, B]
  ): BsonRecordQueryField[M, B] = {
    val rec = f.defaultValue // a hack to get at the embedded record
    new BsonRecordQueryField[M, B](f, _.asDBObject, rec)
  }

  implicit def rbsonRecordFieldToBsonRecordQueryField[
      M <: BsonRecord[M],
      B <: BsonRecord[B]
  ](
      f: RField[B, M]
  ): BsonRecordQueryField[M, B] = {
    // a hack to get at the embedded record
    val owner = f.owner
    if (f.name.indexOf('.') >= 0) {
      val fieldName = f.name.takeWhile(_ != '.')
      val field = owner.fieldByName(fieldName).openOr(sys.error("Error getting field "+fieldName+" for "+owner))
      val typedField = field.asInstanceOf[BsonRecordListField[M, B]]
       // a gross hack to get at the embedded record
      val rec: B = typedField.setFromJValue(JArray(JInt(0) :: Nil)).get.head
      new BsonRecordQueryField[M, B](f, _.asDBObject, rec)
    } else {
      val fieldName = f.name
      val field = owner.fieldByName(fieldName).openOr(sys.error("Error getting field "+fieldName+" for "+owner))
      val typedField = field.asInstanceOf[BsonRecordField[M, B]]
      val rec: B = typedField.defaultValue
      new BsonRecordQueryField[M, B](f, _.asDBObject, rec)
    }
  }

  implicit def bsonRecordListFieldToBsonRecordListQueryField[
      M <: BsonRecord[M],
      B <: BsonRecord[B]
  ](f: BsonRecordListField[M, B]): BsonRecordListQueryField[M, B] = {
    val rec = f.setFromJValue(JArray(JInt(0) :: Nil)).get.head // a gross hack to get at the embedded record
    new BsonRecordListQueryField[M, B](f, rec, _.asDBObject)
  }

  implicit def dateFieldToDateQueryField[M <: BsonRecord[M]]
      (f: Field[java.util.Date, M]): DateQueryField[M] =
    new DateQueryField(f)

  implicit def ccFieldToQueryField[M <: BsonRecord[M], F](f: MongoCaseClassField[M, F]): CaseClassQueryField[F, M] =
    new CaseClassQueryField[F, M](f)

  implicit def ccListFieldToListQueryField[M <: BsonRecord[M], F]
      (f: MongoCaseClassListField[M, F]): CaseClassListQueryField[F, M] =
    new CaseClassListQueryField[F, M](liftField2Recordv2Field(f))

  implicit def doubleFieldtoNumericQueryField[M <: BsonRecord[M], F]
      (f: Field[Double, M]): NumericQueryField[Double, M] =
    new NumericQueryField(f)

  implicit def enumFieldToEnumNameQueryField[M <: BsonRecord[M], F <: Enumeration#Value]
      (f: Field[F, M]): EnumNameQueryField[M, F] =
    new EnumNameQueryField(f)

  implicit def enumFieldToEnumQueryField[M <: BsonRecord[M], F <: Enumeration]
      (f: EnumField[M, F]): EnumIdQueryField[M, F#Value] =
     new EnumIdQueryField(f)

  implicit def enumerationListFieldToEnumerationListQueryField[M <: BsonRecord[M], F <: Enumeration#Value]
      (f: Field[List[F], M]): EnumerationListQueryField[F, M] =
    new EnumerationListQueryField[F, M](f)

  implicit def foreignObjectIdFieldToForeignObjectIdQueryField[M <: BsonRecord[M],
                                                               T <: MongoRecord[T] with ObjectIdKey[T]]
      (f: Field[ObjectId, M] with HasMongoForeignObjectId[T]): ForeignObjectIdQueryField[ObjectId, M, T] =
    new ForeignObjectIdQueryField[ObjectId, M, T](f, _.id)

  implicit def intFieldtoNumericQueryField[M <: BsonRecord[M], F](f: Field[Int, M]): NumericQueryField[Int, M] =
    new NumericQueryField(f)

  implicit def latLongFieldToGeoQueryField[M <: BsonRecord[M]](f: Field[LatLong, M]): GeoQueryField[M] =
    new GeoQueryField(f)

  implicit def listFieldToListQueryField[M <: BsonRecord[M], F: BSONType](f: Field[List[F], M]): ListQueryField[F, M] =
    new ListQueryField[F, M](f)

  implicit def stringsListFieldToStringsListQueryField[M <: BsonRecord[M]](f: Field[List[String], M]): StringsListQueryField[M] =
    new StringsListQueryField[M](f)

  implicit def longFieldtoNumericQueryField[M <: BsonRecord[M], F <: Long](f: Field[F, M]): NumericQueryField[F, M] =
    new NumericQueryField(f)

  implicit def objectIdFieldToObjectIdQueryField[M <: BsonRecord[M], F <: ObjectId](f: Field[F, M]): ObjectIdQueryField[F, M] =
    new ObjectIdQueryField(f)

  implicit def mapFieldToMapQueryField[M <: BsonRecord[M], F](f: Field[Map[String, F], M]): MapQueryField[F, M] =
    new MapQueryField[F, M](f)

  implicit def stringFieldToStringQueryField[F <: String, M <: BsonRecord[M]](f: Field[F, M]): StringQueryField[F, M] =
    new StringQueryField(f)

  // ModifyField implicits
  implicit def fieldToModifyField[M <: BsonRecord[M], F: BSONType](f: Field[F, M]): ModifyField[F, M] = new ModifyField(f)
  implicit def fieldToSafeModifyField[M <: BsonRecord[M], F](f: Field[F, M]): SafeModifyField[F, M] = new SafeModifyField(f)

  implicit def bsonRecordFieldToBsonRecordModifyField[M <: BsonRecord[M], B <: BsonRecord[B]]
      (f: BsonRecordField[M, B]): BsonRecordModifyField[M, B] =
    new BsonRecordModifyField[M, B](f, _.asDBObject)

  implicit def bsonRecordListFieldToBsonRecordListModifyField[
      M <: BsonRecord[M],
      B <: BsonRecord[B]
  ](
      f: BsonRecordListField[M, B]
  )(
      implicit mf: Manifest[B]
  ): BsonRecordListModifyField[M, B] = {
    val rec = f.setFromJValue(JArray(JInt(0) :: Nil)).get.head // a gross hack to get at the embedded record
    new BsonRecordListModifyField[M, B](f, rec, _.asDBObject)(mf)
  }

  implicit def dateFieldToDateModifyField[M <: BsonRecord[M]](f: Field[Date, M]): DateModifyField[M] =
    new DateModifyField(f)

  implicit def ccListFieldToListModifyField[M <: BsonRecord[M], V]
      (f: MongoCaseClassListField[M, V]): CaseClassListModifyField[V, M] =
    new CaseClassListModifyField[V, M](liftField2Recordv2Field(f))

  implicit def doubleFieldToNumericModifyField[M <: BsonRecord[M]]
      (f: Field[Double, M]): NumericModifyField[Double, M] =
    new NumericModifyField(f)

  implicit def enumerationFieldToEnumerationModifyField[M <: BsonRecord[M], F <: Enumeration#Value]
      (f: Field[F, M]): EnumerationModifyField[M, F] =
    new EnumerationModifyField(f)

  implicit def enumerationListFieldToEnumerationListModifyField[M <: BsonRecord[M], F <: Enumeration#Value]
      (f: Field[List[F], M]): EnumerationListModifyField[F, M] =
    new EnumerationListModifyField[F, M](f)

  implicit def intFieldToIntModifyField[M <: BsonRecord[M]]
      (f: Field[Int, M]): NumericModifyField[Int, M] =
    new NumericModifyField(f)

  implicit def latLongFieldToGeoQueryModifyField[M <: BsonRecord[M]](f: Field[LatLong, M]): GeoModifyField[M] =
    new GeoModifyField(f)

  implicit def listFieldToListModifyField[M <: BsonRecord[M], F: BSONType](f: Field[List[F], M]): ListModifyField[F, M] =
    new ListModifyField[F, M](f)

  implicit def longFieldToNumericModifyField[M <: BsonRecord[M]](f: Field[Long, M]): NumericModifyField[Long, M] =
    new NumericModifyField(f)

  implicit def mapFieldToMapModifyField[M <: BsonRecord[M], F](f: Field[Map[String, F], M]): MapModifyField[F, M] =
    new MapModifyField[F, M](f)

  // SelectField implicits
  implicit def mandatoryFieldToSelectField[M <: BsonRecord[M], V]
      (f: Field[V, M] with MandatoryTypedField[V]): SelectField[V, M] =
    new MandatorySelectField(f)

  implicit def optionalFieldToSelectField[M <: BsonRecord[M], V]
      (f: Field[V, M] with OptionalTypedField[V]): SelectField[Option[V], M] =
    new OptionalSelectField(new ROptionalField[V, M] {
      override def name = f.name
      override def owner = f.owner
    })

  implicit def mandatoryLiftField2RequiredRecordv2Field[M <: BsonRecord[M], V](
      f: Field[V, M] with MandatoryTypedField[V]
  ): com.foursquare.field.RequiredField[V, M] = new com.foursquare.field.RequiredField[V, M] {
    override def name = f.name
    override def owner = f.owner
    override def defaultValue = f.defaultValue
  }

  implicit def liftField2Recordv2Field[M <: Record[M], V](f: Field[V, M]): com.foursquare.field.Field[V, M] = new com.foursquare.field.Field[V, M] {
    override def name = f.name
    override def owner = f.owner
  }

  class BsonRecordIsBSONType[T <: BsonRecord[T]] extends BSONType[T] {
    override def asBSONObject(v: T): AnyRef = v.asDBObject
  }

  object _BsonRecordIsBSONType extends BsonRecordIsBSONType[Nothing]

  implicit def BsonRecordIsBSONType[T <: BsonRecord[T]]: BSONType[T] = _BsonRecordIsBSONType.asInstanceOf[BSONType[T]]
}

object LiftRogue extends Rogue with LiftRogue
