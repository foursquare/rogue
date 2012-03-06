// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.recordv2.{
    Field => RField,
    OptionalField => ROptionalField,
    RequiredField => RRequiredField,
    Selectable => RSelectable}
import com.foursquare.rogue.MongoHelpers.{AndCondition, MongoModify}
import java.util.Calendar
import net.liftweb.common.Box
import net.liftweb.json.JsonAST.{JArray, JInt}
import net.liftweb.mongodb.record.{BsonRecord, MongoId, MongoRecord, MongoMetaRecord}
import net.liftweb.record.{Field, MandatoryTypedField, OptionalTypedField, Record}
import net.liftweb.mongodb.record.field.{
    BsonRecordField, BsonRecordListField, MongoCaseClassField, MongoCaseClassListField}
import org.bson.types.ObjectId

trait LiftRogue extends Rogue {
  def OrQuery[M <: MongoRecord[M], R]
      (subqueries: AbstractQuery[M, R, Unordered, Unselected, Unlimited, Unskipped, _]*)
      : AbstractQuery[M, R, Unordered, Unselected, Unlimited, Unskipped, HasOrClause] = {
    subqueries.toList match {
      case Nil => throw new RogueException("No subqueries supplied to OrQuery", null)
      case q :: qs => {
        val orCondition = QueryHelpers.orConditionFromQueries(q :: qs)
        BaseQuery[M, R, Unordered, Unselected, Unlimited, Unskipped, HasOrClause](
          q.meta, q.collectionName, None, None, None, None, None,
          AndCondition(Nil, Some(orCondition)), None, None, None)
      }
    }
  }

  /* Following are a collection of implicit conversions which take a meta-record and convert it to
   * a QueryBuilder. This allows users to write queries as "QueryType where ...".
   */
  implicit def metaRecordToQueryBuilder[M <: MongoRecord[M]]
      (rec: M with MongoMetaRecord[M]): BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] =
    BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](
      rec, rec.collectionName, None, None, None, None, None, AndCondition(Nil, None), None, None, None)

  implicit def metaRecordToModifyQuery[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): AbstractModifyQuery[M] =
      BaseModifyQuery[M](metaRecordToQueryBuilder[M](rec), MongoModify(Nil))

  implicit def metaRecordToIndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): IndexBuilder[M] =
      IndexBuilder(rec)

  /* A couple of implicit conversions that take a query builder, and convert it to a modify. This allows
   * users to write "RecordType.where(...).modify(...)".
   */
  implicit def queryBuilderToModifyQuery[M <: MongoRecord[M], Or <: MaybeHasOrClause]
      (query: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, Or])
      : AbstractModifyQuery[M] = {
    query match {
      // case q: BaseEmptyQuery[_, _, _, _, _, _, _] => new EmptyModifyQuery[M]
      case q: BaseQuery[_, _, _, _, _, _, _] =>
          BaseModifyQuery[M](q.asInstanceOf[BaseQuery[M, M, Unordered, Unselected, Unlimited,
                                                      Unskipped, HasNoOrClause]],
                             MongoModify(Nil))
    }
  }

  implicit def queryBuilderToFindAndModifyQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Or <: MaybeHasOrClause]
      (query: AbstractQuery[M, R, Ord, Sel, Unlimited, Unskipped, Or])
      : AbstractFindAndModifyQuery[M, R] = {
    query match {
      // case q: BaseEmptyQuery[_, _, _, _, _, _, _] => new EmptyFindAndModifyQuery[M, R]
      case q: BaseQuery[_, _, _, _, _, _, _] =>
        BaseFindAndModifyQuery[M, R](q.asInstanceOf[BaseQuery[M, R, Ord, Sel, Unlimited,
                                                              Unskipped, HasNoOrClause]],
                                     MongoModify(Nil))
    }
  }
  implicit def queryToLiftQuery[
      M <: MongoRecord[_],
      R,
      Ord <: MaybeOrdered,
      Sel <: MaybeSelected,
      Lim <: MaybeLimited,
      Sk <: MaybeSkipped,
      Or <: MaybeHasOrClause
  ](
      query: BaseQuery[M, R, Ord, Sel, Lim, Sk, Or]
  ): LiftQuery[M with MongoMetaRecord[_], R, Ord, Sel, Lim, Sk, Or] = {
    LiftQuery(
        query.asInstanceOf[BaseQuery[M with MongoMetaRecord[_], R, Ord, Sel, Lim, Sk, Or]],
        ConcreteLiftQueryExecutor
    )
  }

  implicit def modifyQueryToLiftModifyQuery[M <: MongoRecord[_]](
      query: BaseModifyQuery[M]
  ): LiftModifyQuery[M with MongoMetaRecord[_]] = {
    LiftModifyQuery(
        query.asInstanceOf[BaseModifyQuery[M with MongoMetaRecord[_]]],
        ConcreteLiftQueryExecutor
    )
  }

  implicit def findAndModifyQueryToLiftFindAndModifyQuery[M <: MongoRecord[_], R](
      query: BaseFindAndModifyQuery[M, R]
  ): LiftFindAndModifyQuery[M with MongoMetaRecord[_], R] = {
    LiftFindAndModifyQuery(
        query.asInstanceOf[BaseFindAndModifyQuery[M with MongoMetaRecord[_], R]],
        ConcreteLiftQueryExecutor
    )
  }

  implicit def metaRecordToLiftQuery[M <: MongoRecord[M]](
      rec: M with MongoMetaRecord[M]
  ): LiftQuery[M with MongoMetaRecord[_], M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
    val queryBuilder = metaRecordToQueryBuilder(rec)
    val liftQuery = queryToLiftQuery(queryBuilder)
    liftQuery
  }

  implicit def fieldToQueryField[M <: MongoRecord[M], F](f: Field[F, M]): QueryField[F, M] = new QueryField(f)

  implicit def bsonRecordFieldToBsonRecordQueryField[
      M <: MongoRecord[M],
      B <: BsonRecord[B]
  ](
      f: BsonRecordField[M, B]
  ): BsonRecordQueryField[M, B] = {
    val rec = f.defaultValue // a hack to get at the embedded record
    new BsonRecordQueryField[M, B](f, _.asDBObject, rec)
  }

  implicit def rbsonRecordFieldToBsonRecordQueryField[
      M <: MongoRecord[M],
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
      val rec: B = typedField.setFromJValue(JArray(JInt(0) :: Nil)).open_!.head
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
      M <: MongoRecord[M],
      B <: BsonRecord[B]
  ](f: BsonRecordListField[M, B]): BsonRecordListQueryField[M, B] = {
    val rec = f.setFromJValue(JArray(JInt(0) :: Nil)).open_!.head // a gross hack to get at the embedded record
    new BsonRecordListQueryField[M, B](f, rec, _.asDBObject)
  }

  implicit def calendarFieldToCalendarQueryField[M <: MongoRecord[M]]
      (f: Field[java.util.Calendar, M]): CalendarQueryField[M] =
    new CalendarQueryField(f)

  implicit def ccFieldToQueryField[M <: MongoRecord[M], F](f: MongoCaseClassField[M, F]): CaseClassQueryField[F, M] =
    new CaseClassQueryField[F, M](f)

  implicit def ccListFieldToListQueryField[M <: MongoRecord[M], F]
      (f: MongoCaseClassListField[M, F]): CaseClassListQueryField[F, M] =
    new CaseClassListQueryField[F, M](liftField2Recordv2Field(f))

  implicit def doubleFieldtoNumericQueryField[M <: MongoRecord[M], F]
      (f: Field[Double, M]): NumericQueryField[Double, M] =
    new NumericQueryField(f)

  implicit def enumerationFieldToEnumerationQueryField[M <: MongoRecord[M], F <: Enumeration#Value]
      (f: Field[F, M]): EnumerationQueryField[M, F] =
    new EnumerationQueryField(f)

  implicit def enumerationListFieldToEnumerationListQueryField[M <: MongoRecord[M], F <: Enumeration#Value]
      (f: Field[List[F], M]): EnumerationListQueryField[F, M] =
    new EnumerationListQueryField[F, M](f)

  implicit def foreignObjectIdFieldToForeignObjectIdQueryField[M <: MongoRecord[M],
                                                               T <: MongoRecord[T] with MongoId[T]]
      (f: Field[ObjectId, M] with HasMongoForeignObjectId[T]): ForeignObjectIdQueryField[M, T] =
    new ForeignObjectIdQueryField[M, T](f, _.id)

  implicit def intFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Int, M]): NumericQueryField[Int, M] =
    new NumericQueryField(f)

  implicit def latLongFieldToGeoQueryField[M <: MongoRecord[M]](f: Field[LatLong, M]): GeoQueryField[M] =
    new GeoQueryField(f)

  implicit def listFieldToListQueryField[M <: MongoRecord[M], F](f: Field[List[F], M]): ListQueryField[F, M] =
    new ListQueryField[F, M](f)

  implicit def longFieldtoNumericQueryField[M <: MongoRecord[M]](f: Field[Long, M]): NumericQueryField[Long, M] =
    new NumericQueryField(f)

  implicit def objectIdFieldToObjectIdQueryField[M <: MongoRecord[M], F](f: Field[ObjectId, M])
      : ObjectIdQueryField[M] =
    new ObjectIdQueryField(f)

  implicit def mapFieldToMapQueryField[M <: MongoRecord[M], F](f: Field[Map[String, F], M]): MapQueryField[F, M] =
    new MapQueryField[F, M](f)

  implicit def stringFieldToStringQueryField[M <: MongoRecord[M]](f: Field[String, M]): StringQueryField[M] =
    new StringQueryField(f)

  // ModifyField implicits
  implicit def fieldToModifyField[M <: MongoRecord[M], F](f: Field[F, M]): ModifyField[F, M] = new ModifyField(f)

  implicit def bsonRecordFieldToBsonRecordModifyField[M <: MongoRecord[M], B <: BsonRecord[B]]
      (f: BsonRecordField[M, B]): BsonRecordModifyField[M, B] =
    new BsonRecordModifyField[M, B](f, _.asDBObject)

  implicit def bsonRecordListFieldToBsonRecordListModifyField[
      M <: MongoRecord[M],
      B <: BsonRecord[B]
  ](
      f: BsonRecordListField[M, B]
  )(
      implicit mf: Manifest[B]
  ): BsonRecordListModifyField[M, B] = {
    val rec = f.setFromJValue(JArray(JInt(0) :: Nil)).open_!.head // a gross hack to get at the embedded record
    new BsonRecordListModifyField[M, B](f, rec, _.asDBObject)(mf)
  }

  implicit def calendarFieldToCalendarModifyField[M <: MongoRecord[M]](f: Field[Calendar, M]): CalendarModifyField[M] =
    new CalendarModifyField(f)

  implicit def ccListFieldToListModifyField[M <: MongoRecord[M], V]
      (f: MongoCaseClassListField[M, V]): CaseClassListModifyField[V, M] =
    new CaseClassListModifyField[V, M](liftField2Recordv2Field(f))

  implicit def doubleFieldToNumericModifyField[M <: MongoRecord[M]]
      (f: Field[Double, M]): NumericModifyField[Double, M] =
    new NumericModifyField(f)

  implicit def enumerationFieldToEnumerationModifyField[M <: MongoRecord[M], F <: Enumeration#Value]
      (f: Field[F, M]): EnumerationModifyField[M, F] =
    new EnumerationModifyField(f)

  implicit def enumerationListFieldToEnumerationListModifyField[M <: MongoRecord[M], F <: Enumeration#Value]
      (f: Field[List[F], M]): EnumerationListModifyField[F, M] =
    new EnumerationListModifyField[F, M](f)

  implicit def intFieldToIntModifyField[M <: MongoRecord[M]]
      (f: Field[Int, M]): NumericModifyField[Int, M] =
    new NumericModifyField(f)

  implicit def latLongFieldToGeoQueryModifyField[M <: MongoRecord[M]](f: Field[LatLong, M]): GeoModifyField[M] =
    new GeoModifyField(f)

  implicit def listFieldToListModifyField[M <: MongoRecord[M], F](f: Field[List[F], M]): ListModifyField[F, M] =
    new ListModifyField[F, M](f)

  implicit def longFieldToNumericModifyField[M <: MongoRecord[M]](f: Field[Long, M]): NumericModifyField[Long, M] =
    new NumericModifyField(f)

  implicit def mapFieldToMapModifyField[M <: MongoRecord[M], F](f: Field[Map[String, F], M]): MapModifyField[F, M] =
    new MapModifyField[F, M](f)

  // SelectField implicits
  implicit def mandatoryFieldToSelectField[M <: MongoRecord[M], V]
      (f: Field[V, M] with MandatoryTypedField[V]): SelectField[V, M] =
    new MandatorySelectField(f)

  implicit def optionalFieldToSelectField[M <: MongoRecord[M], V]
      (f: Field[V, M] with OptionalTypedField[V]): SelectField[Box[V], M] =
    new OptionalSelectField(new ROptionalField[V, M] {
      override def name = f.name
      override def owner = f.owner
    })

  implicit def mandatoryLiftField2RequiredRecordv2Field[M <: MongoRecord[M], V](
      f: Field[V, M] with MandatoryTypedField[V]
  ): com.foursquare.recordv2.RequiredField[V, M] = new com.foursquare.recordv2.RequiredField[V, M] {
    override def name = f.name
    override def owner = f.owner
    override def defaultValue = f.defaultValue
  }

  implicit def liftField2Recordv2Field[M <: Record[M], V](f: Field[V, M]): com.foursquare.recordv2.Field[V, M] = new com.foursquare.recordv2.Field[V, M] {
    override def name = f.name
    override def owner = f.owner
  }
}

object LiftRogue extends LiftRogue

// object LiftQueryS {
//   def apply[
//       R <: MongoRecord[R]
//   ](
//       meta: R with MongoMetaRecord[R]
//   ): BaseQuery[R, R, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] =
//     BaseQuery[R, R, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](
//       meta, meta.collectionName, None, None, None, None, None, AndCondition(Nil, None), None, None, None)
// }
