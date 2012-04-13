// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.recordv2.{
    Field => RField,
    OptionalField => ROptionalField,
    RequiredField => RRequiredField,
    Selectable => RSelectable}
import com.foursquare.rogue.MongoHelpers.{AndCondition, MongoModify}
import java.util.Calendar
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
  implicit def rfieldToQueryField[M, F](f: RField[F, M]): QueryField[F, M] = new QueryField(f)

  implicit def rcalendarFieldToCalendarQueryField[M, F]
      (f: RField[java.util.Calendar, M]): CalendarQueryField[M] =
    new CalendarQueryField(f)

  implicit def rdoubleFieldtoNumericQueryField[M, F]
      (f: RField[Double, M]): NumericQueryField[Double, M] =
    new NumericQueryField(f)

  implicit def renumerationFieldToEnumerationQueryField[M, F <: Enumeration#Value]
      (f: RField[F, M]): EnumerationQueryField[M, F] =
    new EnumerationQueryField(f)

  implicit def renumerationListFieldToEnumerationListQueryField[M, F <: Enumeration#Value]
      (f: RField[List[F], M]): EnumerationListQueryField[F, M] =
    new EnumerationListQueryField[F, M](f)

  implicit def rintFieldtoNumericQueryField[M, F](f: RField[Int, M]): NumericQueryField[Int, M] =
    new NumericQueryField(f)

  implicit def rlatLongFieldToGeoQueryField[M](f: RField[LatLong, M]): GeoQueryField[M] =
    new GeoQueryField(f)

  implicit def rlistFieldToListQueryField[M, F](f: RField[List[F], M]): ListQueryField[F, M] =
    new ListQueryField[F, M](f)

  implicit def rlongFieldtoNumericQueryField[M](f: RField[Long, M]): NumericQueryField[Long, M] =
    new NumericQueryField(f)

  implicit def robjectIdFieldToObjectIdQueryField[M, F](f: RField[ObjectId, M])
      : ObjectIdQueryField[M] =
    new ObjectIdQueryField(f)

  implicit def rmapFieldToMapQueryField[M, F](f: RField[Map[String, F], M]): MapQueryField[F, M] =
    new MapQueryField[F, M](f)

  implicit def rstringFieldToStringQueryField[M](f: RField[String, M]): StringQueryField[M] =
    new StringQueryField(f)

  // ModifyField implicits
  implicit def rfieldToModifyField[M, F](f: RField[F, M]): ModifyField[F, M] = new ModifyField(f)

  implicit def rcalendarFieldToCalendarModifyField[M](f: RField[Calendar, M]): CalendarModifyField[M] =
    new CalendarModifyField(f)

  implicit def rdoubleFieldToNumericModifyField[M]
      (f: RField[Double, M]): NumericModifyField[Double, M] =
    new NumericModifyField(f)

  implicit def renumerationFieldToEnumerationModifyField[M, F <: Enumeration#Value]
      (f: RField[F, M]): EnumerationModifyField[M, F] =
    new EnumerationModifyField(f)

  implicit def renumerationListFieldToEnumerationListModifyField[M, F <: Enumeration#Value]
      (f: RField[List[F], M]): EnumerationListModifyField[F, M] =
    new EnumerationListModifyField[F, M](f)

  implicit def rintFieldToIntModifyField[M]
      (f: RField[Int, M]): NumericModifyField[Int, M] =
    new NumericModifyField(f)

  implicit def rlatLongFieldToGeoQueryModifyField[M](f: RField[LatLong, M]): GeoModifyField[M] =
    new GeoModifyField(f)

  implicit def rlistFieldToListModifyField[M, F](f: RField[List[F], M]): ListModifyField[F, M] =
    new ListModifyField[F, M](f)

  implicit def rlongFieldToNumericModifyField[M](f: RField[Long, M]): NumericModifyField[Long, M] =
    new NumericModifyField(f)

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
