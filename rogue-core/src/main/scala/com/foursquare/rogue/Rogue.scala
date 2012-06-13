// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.field.{
    Field => RField,
    OptionalField => ROptionalField,
    RequiredField => RRequiredField}
import com.foursquare.rogue.MongoHelpers.{AndCondition, MongoModify}
import com.mongodb.DBObject
import java.util.{Calendar, Date}
import net.liftweb.common.Box
import org.bson.types.ObjectId

/**
 * A utility trait containing typing shorthands, and a collection of implicit conversions that make query
 * syntax much simpler.
 *
 *@see AbstractQuery for an example of the use of implicit conversions.
 */
trait Rogue {
  type Query[T] =
    BaseQuery[T, T, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]

  type OrderedQuery[T] =
    BaseQuery[T, T, Ordered, Unselected, Unlimited, Unskipped, HasNoOrClause]

  // type PaginatedQuery[T <: MongoRecord[T]] = BasePaginatedQuery[T, T]
  type ModifyQuery[T] = BaseModifyQuery[T]
  type GenericQuery[M, R] = BaseQuery[M, R, _, _, _, _, _]
  type GenericBaseQuery[M, R] = GenericQuery[M, R]

  object Asc extends IndexModifier(1)
  object Desc extends IndexModifier(-1)
  object TwoD extends IndexModifier("2d")

  /**
   * Iteratee helper classes
   * @tparam S state type
   */
  object Iter {
    sealed trait Command[S] {
      def state: S
    }
    case class Continue[S](state: S) extends Command[S]
    case class Return[S](state: S) extends Command[S]

    sealed trait Event[+R]
    case class Item[R](r: R) extends Event[R]
    case class Error(e: Exception) extends Event[Nothing]
    case object EOF extends Event[Nothing]
  }

  // QueryField implicits
  // implicit def rfieldToQueryField[M, F](f: RField[F, M]): QueryField[F, M] = new QueryField(f)

  implicit def rbooleanFieldtoQueryField[M](f: RField[Boolean, M]): QueryField[Boolean, M] = new QueryField(f)
  implicit def rcharFieldtoQueryField[M](f: RField[Char, M]): QueryField[Char, M] = new QueryField(f)

  implicit def rbyteFieldtoNumericQueryField[M](f: RField[Byte, M]): NumericQueryField[Byte, M] = new NumericQueryField(f)
  implicit def rshortFieldtoNumericQueryField[M](f: RField[Short, M]): NumericQueryField[Short, M] = new NumericQueryField(f)
  implicit def rintFieldtoNumericQueryField[M](f: RField[Int, M]): NumericQueryField[Int, M] = new NumericQueryField(f)
  implicit def rlongFieldtoNumericQueryField[M](f: RField[Long, M]): NumericQueryField[Long, M] = new NumericQueryField(f)
  implicit def rfloatFieldtoNumericQueryField[M](f: RField[Float, M]): NumericQueryField[Float, M] = new NumericQueryField(f)
  implicit def rdoubleFieldtoNumericQueryField[M](f: RField[Double, M]): NumericQueryField[Double, M] = new NumericQueryField(f)

  implicit def rstringFieldToStringQueryField[M](f: RField[String, M]): StringQueryField[M] = new StringQueryField(f)
  implicit def robjectIdFieldToObjectIdQueryField[M](f: RField[ObjectId, M]): ObjectIdQueryField[M] = new ObjectIdQueryField(f)
  implicit def rdateFieldToQueryField[M](f: RField[Date, M]): QueryField[Date, M] = new QueryField(f)
  implicit def rcalendarFieldToCalendarQueryField[M](f: RField[Calendar, M]): CalendarQueryField[M] = new CalendarQueryField(f)
  implicit def rdbobjectFieldToQueryField[M](f: RField[DBObject, M]): QueryField[DBObject, M] = new QueryField(f)

  implicit def renumNameFieldToEnumNameQueryField[M, F <: Enumeration#Value](f: RField[F, M]): EnumNameQueryField[M, F] = new EnumNameQueryField(f)
  implicit def renumerationListFieldToEnumerationListQueryField[M, F <: Enumeration#Value](f: RField[List[F], M]): EnumerationListQueryField[F, M] = new EnumerationListQueryField[F, M](f)
  implicit def rlatLongFieldToGeoQueryField[M](f: RField[LatLong, M]): GeoQueryField[M] = new GeoQueryField(f)
  implicit def rlistFieldToListQueryField[M, F](f: RField[List[F], M]): ListQueryField[F, M] = new ListQueryField[F, M](f)
  implicit def rmapFieldToMapQueryField[M, F](f: RField[Map[String, F], M]): MapQueryField[F, M] = new MapQueryField[F, M](f)

  /** ModifyField implicits
    *
    * These are dangerous in the general case, unless the field type can be safely serialized
    * or the field class handles necessary serialization. We specialize some safe cases.
    **/
  implicit def booleanRFieldToModifyField[M](f: RField[Boolean, M]): ModifyField[Boolean, M] = new ModifyField(f)
  implicit def charRFieldToModifyField[M](f: RField[Char, M]): ModifyField[Char, M] = new ModifyField(f)

  implicit def byteRFieldToModifyField[M](f: RField[Byte, M]): NumericModifyField[Byte, M] = new NumericModifyField(f)
  implicit def shortRFieldToModifyField[M](f: RField[Short, M]): NumericModifyField[Short, M] = new NumericModifyField(f)
  implicit def intRFieldToModifyField[M](f: RField[Int, M]): NumericModifyField[Int, M] = new NumericModifyField(f)
  implicit def longRFieldToModifyField[M](f: RField[Long, M]): NumericModifyField[Long, M] = new NumericModifyField(f)
  implicit def floatRFieldToModifyField[M](f: RField[Float, M]): NumericModifyField[Float, M] = new NumericModifyField(f)
  implicit def doubleRFieldToModifyField[M](f: RField[Double, M]): NumericModifyField[Double, M] = new NumericModifyField(f)

  implicit def stringRFieldToModifyField[M](f: RField[String, M]): ModifyField[String, M] = new ModifyField(f)
  implicit def objectidRFieldToModifyField[M](f: RField[ObjectId, M]): ModifyField[ObjectId, M] = new ModifyField(f)
  implicit def dateRFieldToModifyField[M](f: RField[Date, M]): ModifyField[Date, M] = new ModifyField(f)

  implicit def rcalendarFieldToCalendarModifyField[M](f: RField[Calendar, M]): CalendarModifyField[M] =
    new CalendarModifyField(f)

  implicit def renumerationFieldToEnumerationModifyField[M, F <: Enumeration#Value]
      (f: RField[F, M]): EnumerationModifyField[M, F] =
    new EnumerationModifyField(f)

  implicit def renumerationListFieldToEnumerationListModifyField[M, F <: Enumeration#Value]
      (f: RField[List[F], M]): EnumerationListModifyField[F, M] =
    new EnumerationListModifyField[F, M](f)

  implicit def rlatLongFieldToGeoQueryModifyField[M](f: RField[LatLong, M]): GeoModifyField[M] =
    new GeoModifyField(f)

  implicit def rlistFieldToListModifyField[M, F](f: RField[List[F], M]): ListModifyField[F, M] =
    new ListModifyField[F, M](f)

  implicit def rmapFieldToMapModifyField[M, F](f: RField[Map[String, F], M]): MapModifyField[F, M] =
    new MapModifyField[F, M](f)

  // SelectField implicits
  implicit def roptionalFieldToSelectField[M, V](
      f: ROptionalField[V, M]
  ): SelectField[Box[V], M] = new OptionalSelectField(f)

  class Flattened[A, B]
  implicit def anyValIsFlattened[A <: AnyVal]: Flattened[A, A] = new Flattened[A, A]
  implicit def stringIsFlattened[A <: String]: Flattened[A, A] = new Flattened[A, A]
  implicit def objectIdIsFlattened[A <: ObjectId]: Flattened[A, A] = new Flattened[A, A]
  implicit def enumIsFlattened[A <: Enumeration#Value]: Flattened[A, A] = new Flattened[A, A]
  implicit def recursiveFlatten[A, B](implicit ev: Flattened[A, B]) = new Flattened[List[A], B]
}

object Rogue extends Rogue
