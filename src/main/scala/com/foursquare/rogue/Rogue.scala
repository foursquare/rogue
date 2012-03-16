// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.{AndCondition, MongoModify}
import java.util.Calendar
import net.liftweb.common.Box
import net.liftweb.mongodb.record.{BsonRecord, MongoId, MongoRecord, MongoMetaRecord}
import net.liftweb.record.{Field, MandatoryTypedField, OptionalTypedField}
import net.liftweb.mongodb.record.field.{
    BsonRecordField, BsonRecordListField, MongoCaseClassField, MongoCaseClassListField}
import org.bson.types.ObjectId

/**
 * A utility trait containing typing shorthands, and a collection of implicit conversions that make query
 * syntax much simpler.
 *
 *@see BaseQuery for an example of the use of implicit conversions.
 */
trait Rogue {

  type Query[T <: MongoRecord[T]] =
    AbstractQuery[T, T, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]

  type OrderedQuery[T <: MongoRecord[T]] =
    AbstractQuery[T, T, Ordered, Unselected, Unlimited, Unskipped, HasNoOrClause]

  type PaginatedQuery[T <: MongoRecord[T]] = BasePaginatedQuery[T, T]

  type ModifyQuery[T <: MongoRecord[T]] = BaseModifyQuery[T]

  type GenericQuery[M <: MongoRecord[M], R] = AbstractQuery[M, R, _, _, _, _, _]
  type GenericBaseQuery[M <: MongoRecord[M], R] = BaseQuery[M, R, _, _, _, _, _]

  def OrQuery[M <: MongoRecord[M]]
      (subqueries: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _]*)
      : AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasOrClause] = {
    subqueries.toList match {
      case Nil => throw new RogueException("No subqueries supplied to OrQuery", null)
      case q :: qs => {
        val orCondition = QueryHelpers.orConditionFromQueries(q :: qs)
        BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasOrClause](
          q.meta, None, None, None, None, None,
          AndCondition(Nil, Some(orCondition)), None, None, None, false)
      }
    }
  }

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

  /**
   * Following are a collection of implicit conversions which take a meta-record and convert it to
   * a QueryBuilder. This allows users to write queries as "QueryType where ...".
   */
  implicit def metaRecordToQueryBuilder[M <: MongoRecord[M]]
      (rec: M with MongoMetaRecord[M]): BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] =
    BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](
      rec, None, None, None, None, None, AndCondition(Nil, None), None, None, None, false)

  implicit def metaRecordToModifyQuery[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): AbstractModifyQuery[M] =
      BaseModifyQuery(metaRecordToQueryBuilder(rec), MongoModify(Nil))

  implicit def metaRecordToIndexBuilder[M <: MongoRecord[M]](rec: M with MongoMetaRecord[M]): IndexBuilder[M] =
      IndexBuilder(rec)
  implicit def metaRecordToIndexEnforcer[M <: MongoRecord[M]](meta: M with MongoMetaRecord[M] with IndexedRecord[M]): IndexEnforcerBuilder[M] =
      new IndexEnforcerBuilder(meta)


  /* A couple of implicit conversions that take a query builder, and convert it to a modify. This allows
   * users to write "RecordType.where(...).modify(...)".
   */
  implicit def queryBuilderToModifyQuery[M <: MongoRecord[M], Or <: MaybeHasOrClause]
      (query: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, Or])
      : AbstractModifyQuery[M] = {
    BaseModifyQuery[M](query, MongoModify(Nil))
  }

  implicit def queryBuilderToFindAndModifyQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Or <: MaybeHasOrClause]
      (query: AbstractQuery[M, R, Ord, Sel, Unlimited, Unskipped, Or])
      : AbstractFindAndModifyQuery[M, R] = {
    BaseFindAndModifyQuery[M, R](query, MongoModify(Nil))
  }

  // QueryField implicits
  implicit def fieldToQueryField[M <: BsonRecord[M], F](f: Field[F, M]): QueryField[F, M] = new QueryField(f)

  implicit def bsonRecordFieldToBsonRecordQueryField[M <: BsonRecord[M], B <: BsonRecord[B]]
      (f: BsonRecordField[M, B]): BsonRecordQueryField[M, B] =
    new BsonRecordQueryField[M, B](f)

  implicit def bsonRecordListFieldToBsonRecordListQueryField[M <: BsonRecord[M], B <: BsonRecord[B]]
      (f: BsonRecordListField[M, B]) =
    new BsonRecordListQueryField[M, B](f)

  implicit def calendarFieldToCalendarQueryField[M <: BsonRecord[M], F]
      (f: Field[java.util.Calendar, M]): CalendarQueryField[M] =
    new CalendarQueryField(f)

  implicit def ccFieldToQueryField[M <: BsonRecord[M], F](f: MongoCaseClassField[M, F]): CaseClassQueryField[F, M] =
    new CaseClassQueryField[F, M](f)

  implicit def ccListFieldToListQueryField[M <: BsonRecord[M], F]
      (f: MongoCaseClassListField[M, F]): CaseClassListQueryField[F, M] =
    new CaseClassListQueryField[F, M](f)

  implicit def doubleFieldtoNumericQueryField[M <: BsonRecord[M], F]
      (f: Field[Double, M]): NumericQueryField[Double, M] =
    new NumericQueryField(f)

  implicit def enumerationFieldToEnumerationQueryField[M <: BsonRecord[M], F <: Enumeration#Value]
      (f: Field[F, M]): EnumerationQueryField[M, F] =
    new EnumerationQueryField(f)

  implicit def enumerationListFieldToEnumerationListQueryField[M <: BsonRecord[M], F <: Enumeration#Value]
      (f: Field[List[F], M]): EnumerationListQueryField[F, M] =
    new EnumerationListQueryField[F, M](f)

  implicit def foreignObjectIdFieldToForeignObjectIdQueryField[M <: BsonRecord[M],
                                                               T <: MongoRecord[T] with MongoId[T]]
      (f: Field[ObjectId, M] with HasMongoForeignObjectId[T]): ForeignObjectIdQueryField[M, T] =
    new ForeignObjectIdQueryField(f)

  implicit def intFieldtoNumericQueryField[M <: BsonRecord[M], F](f: Field[Int, M]): NumericQueryField[Int, M] =
    new NumericQueryField(f)

  implicit def latLongFieldToGeoQueryField[M <: BsonRecord[M]](f: Field[LatLong, M]): GeoQueryField[M] =
    new GeoQueryField(f)

  class Flattened[A, B]
  implicit def anyValIsFlattened[A <: AnyVal]: Flattened[A, A] = new Flattened[A, A]
  implicit def stringIsFlattened[A <: String]: Flattened[A, A] = new Flattened[A, A]
  implicit def objectIdIsFlattened[A <: ObjectId]: Flattened[A, A] = new Flattened[A, A]
  implicit def enumIsFlattened[A <: Enumeration#Value]: Flattened[A, A] = new Flattened[A, A]
  implicit def recursiveFlatten[A, B](implicit ev: Flattened[A, B]) = new Flattened[List[A], B]

  implicit def listFieldToListQueryField[M <: BsonRecord[M], F](f: Field[List[F], M]): ListQueryField[F, M] =
    new ListQueryField[F, M](f)

  implicit def longFieldtoNumericQueryField[M <: BsonRecord[M], F](f: Field[Long, M]): NumericQueryField[Long, M] =
    new NumericQueryField(f)

  implicit def objectIdFieldToObjectIdQueryField[M <: BsonRecord[M], F](f: Field[ObjectId, M])
      : ObjectIdQueryField[M] =
    new ObjectIdQueryField(f)

  implicit def mapFieldToMapQueryField[M <: BsonRecord[M], F](f: Field[Map[String, F], M]): MapQueryField[F, M] =
    new MapQueryField[F, M](f)

  implicit def stringFieldToStringQueryField[M <: BsonRecord[M]](f: Field[String, M]): StringQueryField[M] =
    new StringQueryField(f)

  // ModifyField implicits
  implicit def fieldToModifyField[M <: BsonRecord[M], F](f: Field[F, M]): ModifyField[F, M] = new ModifyField(f)

  implicit def bsonRecordFieldToBsonRecordModifyField[M <: BsonRecord[M], B <: BsonRecord[B]]
      (f: BsonRecordField[M, B]) =
    new BsonRecordModifyField[M, B](f)

  implicit def bsonRecordListFieldToBsonRecordListModifyField[M <: BsonRecord[M], B <: BsonRecord[B]]
      (f: BsonRecordListField[M, B])(implicit mf: Manifest[B]): BsonRecordListModifyField[M, B] =
    new BsonRecordListModifyField[M, B](f)(mf)

  implicit def calendarFieldToCalendarModifyField[M <: BsonRecord[M]](f: Field[Calendar, M]): CalendarModifyField[M] =
    new CalendarModifyField(f)

  implicit def ccListFieldToListModifyField[M <: BsonRecord[M], V]
      (f: MongoCaseClassListField[M, V]): CaseClassListModifyField[V, M] =
    new CaseClassListModifyField[V, M](f)

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

  implicit def listFieldToListModifyField[M <: BsonRecord[M], F](f: Field[List[F], M]): ListModifyField[F, M] =
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
      (f: Field[V, M] with OptionalTypedField[V]): SelectField[Box[V], M] =
    new OptionalSelectField(f)
}

object Rogue extends Rogue
