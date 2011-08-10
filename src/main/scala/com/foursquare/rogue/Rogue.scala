// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers._
import java.util.Calendar
import net.liftweb.common._
import net.liftweb.mongodb.record._
import net.liftweb.record._
import net.liftweb.mongodb.record.field.{BsonRecordListField, MongoCaseClassField, MongoCaseClassListField}
import org.bson.types.ObjectId

trait Rogue {
  type Query[T <: MongoRecord[T]] = AbstractQuery[T, T, Unordered, Unselected, Unlimited, Unskipped]
  type OrderedQuery[T <: MongoRecord[T]] = AbstractQuery[T, T, Ordered, Unselected, Unlimited, Unskipped]
  type PaginatedQuery[T <: MongoRecord[T]] = BasePaginatedQuery[T, T]
  type EmptyQuery[T <: MongoRecord[T]] = BaseEmptyQuery[T, T, Unordered, Unselected, Unlimited, Unskipped]
  type ModifyQuery[T <: MongoRecord[T]] = AbstractModifyQuery[T]

  object Asc extends IndexModifier(1)
  object Desc extends IndexModifier(-1)
  object TwoD extends IndexModifier("2d")

  implicit def metaRecordToQueryBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped] =
    BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped](rec, None, None, None, None, None, AndCondition(Nil), None, None)
  implicit def metaRecordToModifyQuery[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): AbstractModifyQuery[M] =
    BaseModifyQuery(metaRecordToQueryBuilder(rec), MongoModify(Nil))
  implicit def metaRecordToIndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): IndexBuilder[M] =
    IndexBuilder(rec)
  implicit def queryBuilderToModifyQuery[M <: MongoRecord[M]](query: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped]): AbstractModifyQuery[M] = {
    query match {
      case q: BaseEmptyQuery[_, _, _, _, _, _] => new EmptyModifyQuery[M]
      case q: BaseQuery[_, _, _, _, _, _] => BaseModifyQuery[M](q.asInstanceOf[BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped]], MongoModify(Nil))
    }
  }
  implicit def queryBuilderToFindAndModifyQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped](query: AbstractQuery[M, R, Ord, Sel, Lim, Sk]): AbstractFindAndModifyQuery[M, R] = {
    query match {
      case q: BaseEmptyQuery[_, _, _, _, _, _] => new EmptyFindAndModifyQuery[M, R]
      case q: BaseQuery[_, _, _, _, _, _] => BaseFindAndModifyQuery[M, R](q.asInstanceOf[BaseQuery[M, R, Ord, Sel, Lim, Sk]], MongoModify(Nil))
    }
  }

  implicit def fieldToQueryField[M <: MongoRecord[M], F](f: Field[F, M]): QueryField[F, M] = new QueryField(f)
  implicit def latLongFieldToGeoQueryField[M <: MongoRecord[M]](f: Field[LatLong, M]): GeoQueryField[M] = new GeoQueryField(f)
  implicit def listFieldToListQueryField[M <: MongoRecord[M], F](f: Field[List[F], M]): ListQueryField[F, M] = new ListQueryField[F, M](f)
  implicit def ccFieldToQueryField[M <: MongoRecord[M], F](f: MongoCaseClassField[M, F]): CaseClassQueryField[F, M] = new CaseClassQueryField[F, M](f)
  implicit def cclistFieldToListQueryField[M <: MongoRecord[M], F](f: MongoCaseClassListField[M, F]): CaseClassListQueryField[F, M] =
    new CaseClassListQueryField[F, M](f)
  implicit def mapFieldToMapQueryField[M <: MongoRecord[M], F](f: Field[Map[String, F], M]): MapQueryField[F, M] = new MapQueryField[F, M](f)
  implicit def stringFieldToStringQueryField[M <: MongoRecord[M]](f: Field[String, M]): StringQueryField[M] = new StringQueryField(f)
  implicit def objectIdFieldToObjectIdQueryField[M <: MongoRecord[M], F](f: Field[ObjectId, M]): ObjectIdQueryField[M] = new ObjectIdQueryField(f)
  implicit def calendarFieldToCalendarQueryField[M <: MongoRecord[M], F](f: Field[java.util.Calendar, M]): CalendarQueryField[M] = new CalendarQueryField(f)
  implicit def enumerationFieldToEnumerationQueryField[M <: MongoRecord[M], F <: Enumeration#Value](f: Field[F, M]): EnumerationQueryField[M, F] =
    new EnumerationQueryField(f)
  implicit def enumerationListFieldToEnumerationListQueryField[M <: MongoRecord[M], F <: Enumeration#Value](f: Field[List[F], M]): EnumerationListQueryField[F, M] =
    new EnumerationListQueryField[F, M](f)
  implicit def intFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Int, M]): NumericQueryField[Int, M] = new NumericQueryField(f)
  implicit def longFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Long, M]): NumericQueryField[Long, M] = new NumericQueryField(f)
  implicit def doubleFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Double, M]): NumericQueryField[Double, M] = new NumericQueryField(f)

  implicit def fieldToModifyField[M <: MongoRecord[M], F](f: Field[F, M]): ModifyField[F, M] = new ModifyField(f)
  implicit def latLongFieldToGeoQueryModifyField[M <: MongoRecord[M]](f: Field[LatLong, M]): GeoModifyField[M] = new GeoModifyField(f)
  implicit def listFieldToListModifyField[M <: MongoRecord[M], F](f: Field[List[F], M]): ListModifyField[F, M] = new ListModifyField[F, M](f)
  implicit def cclistFieldToListModifyField[M <: MongoRecord[M], V](f: MongoCaseClassListField[M, V]): CaseClassListModifyField[V, M] =
    new CaseClassListModifyField[V, M](f)
  implicit def bsonRecordListFieldToBsonRecordListModifyField[B <: MongoRecord[B], M <: MongoRecord[M]](f: BsonRecordListField[M, B]) =
    new BsonRecordListModifyField[B, M](f)
  implicit def intFieldToIntModifyField[M <: MongoRecord[M]](f: Field[Int, M]): NumericModifyField[Int, M] = new NumericModifyField(f)
  implicit def longFieldToLongModifyField[M <: MongoRecord[M]](f: Field[Long, M]): NumericModifyField[Long, M] = new NumericModifyField(f)
  implicit def enumerationListFieldToEnumerationListModifyField[M <: MongoRecord[M], F <: Enumeration#Value](f: Field[List[F], M]): EnumerationListModifyField[F, M] =
    new EnumerationListModifyField[F, M](f)
  implicit def calendarFieldToCalendarModifyField[M <: MongoRecord[M]](f: Field[Calendar, M]): CalendarModifyField[M] = new CalendarModifyField(f)

  implicit def mandatoryFieldToSelectField[M <: MongoRecord[M], V](f: Field[V, M] with MandatoryTypedField[V]): SelectField[V, M] = new MandatorySelectField(f)
  implicit def optionalFieldToSelectField[M <: MongoRecord[M], V](f: Field[V, M] with OptionalTypedField[V]): SelectField[Box[V], M] = new OptionalSelectField(f)

  implicit def foreignObjectIdFieldToForeignObjectIdQueryField[M <: MongoRecord[M], T <: MongoRecord[T] with MongoId[T]](f: Field[ObjectId, M] with HasMongoForeignObjectId[T]): ForeignObjectIdQueryField[M, T] = new ForeignObjectIdQueryField(f)
}

object Rogue extends Rogue
