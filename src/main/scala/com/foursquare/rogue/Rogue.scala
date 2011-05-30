// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers._
import java.util.Calendar
import net.liftweb.common._
import net.liftweb.mongodb.record._
import net.liftweb.record._
import net.liftweb.mongodb.record.field.{MongoCaseClassField, MongoCaseClassListField}
import org.bson.types.ObjectId

trait Rogue {
  type Query[T <: MongoRecord[T]] = AbstractQuery[T, T, Unordered, Unselected, Unlimited, Unskipped]
  type OrderedQuery[T <: MongoRecord[T]] = AbstractQuery[T, T, Ordered, Unselected, Unlimited, Unskipped]
  type PaginatedQuery[T <: MongoRecord[T]] = BasePaginatedQuery[T, T]
  type EmptyQuery[T <: MongoRecord[T]] = BaseEmptyQuery[T, T, Unordered, Unselected, Unlimited, Unskipped]
  type ModifyQuery[T <: MongoRecord[T]] = AbstractModifyQuery[T]

  implicit def metaRecordToQueryBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]) = BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped](rec, None, None, None, None, None, AndCondition(Nil), None, None)
  implicit def metaRecordToModifyQuery[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]) = BaseModifyQuery(metaRecordToQueryBuilder(rec), MongoModify(Nil))
  implicit def metaRecordToIndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]) = IndexBuilder(rec)
  implicit def queryBuilderToModifyQuery[M <: MongoRecord[M]](query: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped]) = {
    query match {
      case q: BaseEmptyQuery[_, _, _, _, _, _] => new EmptyModifyQuery[M]
      case q: BaseQuery[_, _, _, _, _, _] => BaseModifyQuery[M](q.asInstanceOf[BaseQuery[M, _, _, _, _, _]], MongoModify(Nil))
    }
  }

  implicit def fieldToQueryField[M <: MongoRecord[M], F](f: Field[F, M]) = new QueryField(f)
  implicit def latLongFieldToGeoQueryField[M <: MongoRecord[M]](f: Field[LatLong, M]) = new GeoQueryField(f)
  implicit def listFieldToListQueryField[M <: MongoRecord[M], F](f: Field[List[F], M]) = new ListQueryField[F, M](f)
  implicit def ccFieldToQueryField[M <: MongoRecord[M], F](f: MongoCaseClassField[M, F]) = new CaseClassQueryField[F, M](f)
  implicit def cclistFieldToListQueryField[M <: MongoRecord[M], F](f: MongoCaseClassListField[M, F]) = new CaseClassListQueryField[F, M](f)
  implicit def mapFieldToMapQueryField[M <: MongoRecord[M], F](f: Field[Map[String, F], M]) = new MapQueryField[F, M](f)
  implicit def stringFieldToStringQueryField[M <: MongoRecord[M]](f: Field[String, M]) = new StringQueryField(f)
  implicit def objectIdFieldToObjectIdQueryField[M <: MongoRecord[M], F](f: Field[ObjectId, M]) = new ObjectIdQueryField(f)
  implicit def calendarFieldToCalendarQueryField[M <: MongoRecord[M], F](f: Field[java.util.Calendar, M]) = new CalendarQueryField(f)
  implicit def enumerationFieldToEnumerationQueryField[M <: MongoRecord[M], F <: Enumeration#Value](f: Field[F, M]) = new EnumerationQueryField(f)
  implicit def enumerationListFieldToEnumerationListQueryField[M <: MongoRecord[M], F <: Enumeration#Value](f: Field[List[F], M]) = new EnumerationListQueryField[F, M](f)
  implicit def intFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Int, M]) = new NumericQueryField(f)
  implicit def longFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Long, M]) = new NumericQueryField(f)
  implicit def doubleFieldtoNumericQueryField[M <: MongoRecord[M], F](f: Field[Double, M]) = new NumericQueryField(f)

  implicit def fieldToModifyField[M <: MongoRecord[M], F](f: Field[F, M]) = new ModifyField(f)
  implicit def latLongFieldToGeoQueryModifyField[M <: MongoRecord[M]](f: Field[LatLong, M]) = new GeoModifyField(f)
  implicit def listFieldToListModifyField[M <: MongoRecord[M], F](f: Field[List[F], M]) = new ListModifyField[F, M](f)
  implicit def cclistFieldToListModifyField[M <: MongoRecord[M], V](f: MongoCaseClassListField[M, V]) = new CaseClassListModifyField[V, M](f)
  implicit def intFieldToIntModifyField[M <: MongoRecord[M]](f: Field[Int, M]) = new NumericModifyField(f)
  implicit def longFieldToLongModifyField[M <: MongoRecord[M]](f: Field[Long, M]) = new NumericModifyField(f)
  implicit def enumerationListFieldToEnumerationListModifyField[M <: MongoRecord[M], F <: Enumeration#Value](f: Field[List[F], M]) = new EnumerationListModifyField[F, M](f)
  implicit def calendarFieldToCalendarModifyField[M <: MongoRecord[M]](f: Field[Calendar, M]) = new CalendarModifyField(f)

  implicit def mandatoryFieldToSelectField[M <: MongoRecord[M], V](f: Field[V, M] with MandatoryTypedField[V]): SelectField[V, M] = new MandatorySelectField(f)
  implicit def optionalFieldToSelectField[M <: MongoRecord[M], V](f: Field[V, M] with OptionalTypedField[V]): SelectField[Box[V], M] = new OptionalSelectField(f)

  implicit def intListToLongList(xs: List[Int]): List[Long] = xs.map(_.toLong)
}

object Rogue extends Rogue
