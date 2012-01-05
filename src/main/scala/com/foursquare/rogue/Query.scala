// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import collection.immutable.List._
import com.foursquare.rogue.MongoHelpers._
import com.mongodb.{BasicDBObjectBuilder, DBObject, WriteConcern}
import net.liftweb.common.{Box, Full}
import net.liftweb.mongodb.MongoDB
import net.liftweb.mongodb.record._
import net.liftweb.record.Field
import org.bson.types.BasicBSONList
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

// ***************************************************************************
// *** Phantom types
// ***************************************************************************

abstract sealed class MaybeOrdered
abstract sealed class Ordered extends MaybeOrdered
abstract sealed class Unordered extends MaybeOrdered

abstract sealed class MaybeSelected
abstract sealed class Selected extends MaybeSelected
abstract sealed class Unselected extends MaybeSelected

abstract sealed class MaybeLimited
abstract sealed class Limited extends MaybeLimited
abstract sealed class Unlimited extends MaybeLimited

abstract sealed class MaybeSkipped
abstract sealed class Skipped extends MaybeSkipped
abstract sealed class Unskipped extends MaybeSkipped

abstract sealed class MaybeHasOrClause
abstract sealed class HasOrClause extends MaybeHasOrClause
abstract sealed class HasNoOrClause extends MaybeHasOrClause

sealed trait MaybeIndexed
sealed trait Indexable extends MaybeIndexed
sealed trait IndexScannable extends MaybeIndexed

abstract sealed class NoIndexInfo extends Indexable with IndexScannable
abstract sealed class Index extends Indexable with IndexScannable
abstract sealed class PartialIndexScan extends IndexScannable
abstract sealed class IndexScan extends IndexScannable
abstract sealed class DocumentScan extends MaybeIndexed

case object NoIndexInfo extends NoIndexInfo
case object Index extends Index
case object PartialIndexScan extends PartialIndexScan
case object IndexScan extends IndexScan
case object DocumentScan extends DocumentScan

abstract sealed class MaybeUsedIndex
abstract sealed class UsedIndex extends MaybeUsedIndex
abstract sealed class HasntUsedIndex extends MaybeUsedIndex

// ***************************************************************************
// *** Indexes
// ***************************************************************************

class IndexEnforcerBuilder[M <: MongoRecord[M]](meta: M with MongoMetaRecord[M] with IndexedRecord[M]) {
  type MetaM = M with MongoMetaRecord[M] with IndexedRecord[M]

  def useIndex[F1 <: Field[_, M]](i: MongoIndex1[M, F1, _]): IndexEnforcer1[M, NoIndexInfo, F1, HasntUsedIndex] = {
    new IndexEnforcer1[M, NoIndexInfo, F1, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None))
  }

  def useIndex[F1 <: Field[_, M], F2 <: Field[_, M]](i: MongoIndex2[M, F1, _, F2, _]): IndexEnforcer2[M, NoIndexInfo, F1, F2, HasntUsedIndex] = {
    new IndexEnforcer2[M, NoIndexInfo, F1, F2, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None))
  }

  def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M]](i: MongoIndex3[M, F1, _, F2, _, F3, _]): IndexEnforcer3[M, NoIndexInfo, F1, F2, F3, HasntUsedIndex] = {
    new IndexEnforcer3[M, NoIndexInfo, F1, F2, F3, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None))
  }

  def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M], F4 <: Field[_, M]](i: MongoIndex4[M, F1, _, F2, _, F3, _, F4, _]): IndexEnforcer4[M, NoIndexInfo, F1, F2, F3, F4, HasntUsedIndex] = {
    new IndexEnforcer4[M, NoIndexInfo, F1, F2, F3, F4, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None))
  }

  def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M], F4 <: Field[_, M], F5 <: Field[_, M]](i: MongoIndex5[M, F1, _, F2, _, F3, _, F4, _, F5, _]): IndexEnforcer5[M, NoIndexInfo, F1, F2, F3, F4, F5, HasntUsedIndex] = {
    new IndexEnforcer5[M, NoIndexInfo, F1, F2, F3, F4, F5, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None))
  }

  def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M], F4 <: Field[_, M], F5 <: Field[_, M], F6 <: Field[_, M]](i: MongoIndex6[M, F1, _, F2, _, F3, _, F4, _, F5, _, F6, _]): IndexEnforcer6[M, NoIndexInfo, F1, F2, F3, F4, F5, F6, HasntUsedIndex] = {
    new IndexEnforcer6[M, NoIndexInfo, F1, F2, F3, F4, F5, F6, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None))
  }
}

case class IndexEnforcer1[M <: MongoRecord[M],
                          Ind <: MaybeIndexed,
                          F1 <: Field[_, M],
                          UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
                                             q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
  def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
    q.where(_ => clause(f1Func(meta)))
  }

  def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
    q.and(_ => clause(f1Func(meta)))
  }

  def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
    q.iscan(_ => clause(f1Func(meta)))
  }

  def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
}

case class IndexEnforcer2[M <: MongoRecord[M],
                          Ind <: MaybeIndexed,
                          F1 <: Field[_, M],
                          F2 <: Field[_, M],
                          UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
                                                          q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
  def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer1[M, Index, F2, UsedIndex] = {
    new IndexEnforcer1[M, Index, F2, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
  }

  def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer1[M, Index, F2, UsedIndex] = {
    new IndexEnforcer1[M, Index, F2, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
  }

  def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer1[M, IndexScan, F2, UsedIndex] = {
    new IndexEnforcer1[M, IndexScan, F2, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
  }

  def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F2, UsedIndex] = {
    new IndexEnforcer1[M, IndexScan, F2, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
}

case class IndexEnforcer3[M <: MongoRecord[M],
                          Ind <: MaybeIndexed,
                          F1 <: Field[_, M],
                          F2 <: Field[_, M],
                          F3 <: Field[_, M],
                          UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
                                             q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
  def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer2[M, Index, F2, F3, UsedIndex] = {
    new IndexEnforcer2[M, Index, F2, F3, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
  }

  def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer2[M, Index, F2, F3, UsedIndex] = {
    new IndexEnforcer2[M, Index, F2, F3, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
  }

  def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex] = {
    new IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
  }

  def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex] = {
    new IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F3, UsedIndex] = {
    new IndexEnforcer1[M, IndexScan, F3, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
}

case class IndexEnforcer4[M <: MongoRecord[M],
                          Ind <: MaybeIndexed,
                          F1 <: Field[_, M],
                          F2 <: Field[_, M],
                          F3 <: Field[_, M],
                          F4 <: Field[_, M],
                          UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
                                                           q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
  def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex] = {
    new IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
  }

  def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex] = {
    new IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
  }

  def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex] = {
    new IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
  }

  def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex] = {
    new IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer2[M, IndexScan, F3, F4, UsedIndex] = {
    new IndexEnforcer2[M, IndexScan, F3, F4, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F4, UsedIndex] = {
    new IndexEnforcer1[M, IndexScan, F4, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3, f4Func: M => F4)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
}

case class IndexEnforcer5[M <: MongoRecord[M],
                          Ind <: MaybeIndexed,
                          F1 <: Field[_, M],
                          F2 <: Field[_, M],
                          F3 <: Field[_, M],
                          F4 <: Field[_, M],
                          F5 <: Field[_, M],
                          UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
                                                           q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
  def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex] = {
    new IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
  }

  def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex] = {
    new IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
  }

  def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex] = {
    new IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
  }

  def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex] = {
    new IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer3[M, IndexScan, F3, F4, F5, UsedIndex] = {
    new IndexEnforcer3[M, IndexScan, F3, F4, F5, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer2[M, IndexScan, F4, F5, UsedIndex] = {
    new IndexEnforcer2[M, IndexScan, F4, F5, UsedIndex](meta, q)
  }

  def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3, f4Func: M => F4)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F5, UsedIndex] = {
    new IndexEnforcer1[M, IndexScan, F5, UsedIndex](meta, q)
  }
}

case class IndexEnforcer6[M <: MongoRecord[M],
                          Ind <: MaybeIndexed,
                          F1 <: Field[_, M],
                          F2 <: Field[_, M],
                          F3 <: Field[_, M],
                          F4 <: Field[_, M],
                          F5 <: Field[_, M],
                          F6 <: Field[_, M],
                          UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
                                                          q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
  def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex] = {
    new IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
  }

  def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex] = {
    new IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
  }

  def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer5[M, IndexScan, F2, F3, F4, F5, F6, UsedIndex] = {
    new IndexEnforcer5[M, IndexScan, F2, F3, F4, F5, F6, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
  }

  // IndexEnforcer6 doesn't have methods to scan any later fields in the index
  // because there's no way that we got here via an iscan on a bigger index --
  // there are no bigger indexes. We require that the first column on the index
  // gets used.
}

// ***************************************************************************
// *** Builders
// ***************************************************************************


/**
 * The definition of methods needed to build a query.
 *
 * <p> To construct a query, an instance of a query-builder needs to be created. That's
 * done by using an implicit conversion from an instance of a meta-record. In code, the
 * user writes a query by calling one of the query construction methods on an instance of
 * the query-record meta-record instance. The implicit conversion will construct an appropriate
 * query builder from the meta-record.</p>
 *
 * <p> Query builders are parameterized using a collection of <em>phantom types</em>.
 * For our purposes here, phantom types are types which are inferred by the type system,
 * rather than being explicitly provided by users. The phantom types are inferred by the type
 * system on the basis of what clauses are contained in the query. For example, if there's a
 * ordering clause, that constrains the types so that the type system must infer a type parameter
 * of type "Ordered". This use of phantom types allows the type system to prevent a range of
 * query errors - for example, if two query clauses have incompatible ordering constraints, the
 * type system will reject it.</p>
 *
 * <p> The specific mechanics of the type inference process are based on implicit parameters. A
 * type can only get inferred into an expression based on a parameter. But we don't want people
 * to have to specify parameters explicitly - that would wreck the syntax. Instead, we use implicit
 * parameters. The inference system will find an implicit parameter that is type compatible with
 * what's used in the rest of the expression.
 *
 * @param M the record type being queried.
 * @param R
 * @param Ord a phantom type which defines the sorting behavior of the query.
 * @param Sel a phantom type which defines the selection criteria of the query.
 * @param Lim a phantom type which defines the result-size limit of the query.
 * @param Sk a phantom type which defines the skip-size of the query.
 * @param Or a phantom type which defines whether this query is joined to another query-clause by
 *    an or-clause. The type system will guarantee that the query and its or-connected query
 *    have the same types.
 */
trait AbstractQuery[M <: MongoRecord[M], R,
                    Ord <: MaybeOrdered,
                    Sel <: MaybeSelected,
                    Lim <: MaybeLimited,
                    Sk <: MaybeSkipped,
                    Or <: MaybeHasOrClause] {

  def meta: M with MongoMetaRecord[M]
  def master: MongoMetaRecord[M]

  /**
   * Adds a where clause to a query.
   */
  def where[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  /**
   * Adds another and-connected clause to the query.
   */
  def and[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  /**
   * Adds a scan clause to a query.
   */
  def scan[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  /**
   * Adds an iscan clause to a query.
   */
  def iscan[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
  def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
  def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
  def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  def raw(f: BasicDBObjectBuilder => Unit): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  /**
   * Chains an "or" subquery to the current query.
   *
   * <p> The use of the implicit parameter here is key to how the Rogue type checking
   * mechanics work. In order to attach an "or" clause to a query, the query as it exists
   * must <em>not</em> yet have an or-clause. So the implicit parameter, which carries
   * the phantom type information, must be "HasNoOrClause" before this is called. After it's called,
   * you can see that the "MaybeHasOrClause" type parameter is changed, and is now specifically
   * bound to "HasOrClause", rather than to a type variable.</p>
   */
  def or(subqueries: (M with MongoMetaRecord[M] =>
                      AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _])*)
                      (implicit ev: Or =:= HasNoOrClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause]

   /**
    * Like "or", this uses the Rogue phantom-type/implicit parameter mechanics. To call this
    * method, the query must <em>not</em> yet have an ordering clause attached. This is captured
    * by the implicit parameter being constrained to be "Unordered". After this is called, the
    * type signature of the returned query is updated so that the "MaybeOrdered" type parameter is
    * now Ordered.
    */
  def orderAsc[V](field: M => QueryField[V, M])
                      (implicit ev: Ord =:= Unordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]
  def orderDesc[V](field: M => QueryField[V, M])
                      (implicit ev: Ord =:= Unordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]
  def andAsc[V](field: M => QueryField[V, M])
                      (implicit ev: Ord =:= Ordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]
  def andDesc[V](field: M => QueryField[V, M])
                      (implicit ev: Ord =:= Ordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]

  /**
   * Places a limit on the size of the returned result.
   *
   * <p> Like "or", this uses the Rogue phantom-type/implicit parameter mechanics. To call this
   * method, the query must <em>not</em> yet have a limit clause attached. This is captured
   * by the implicit parameter being constrained to be "Unlimited". After this is called, the
   * type signature of the returned query is updated so that the "MaybeLimited" type parameter is
   * now Limited.</p>
   */
  def limit(n: Int)(implicit ev: Lim =:= Unlimited): AbstractQuery[M, R, Ord, Sel, Limited, Sk, Or]

  def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): AbstractQuery[M, R, Ord, Sel, Limited, Sk, Or]

  /**
   * Adds a skip to the query.
   *
   * <p> Like {@link or}, this uses the Rogue phantom-type/implicit parameter mechanics. To call this
   * method, the query must <em>not</em> yet have a skip clause attached. This is captured
   * by the implicit parameter being constrained to be {@link Unskipped}. After this is called, the
   * type signature of the returned query is updated so that the {@link MaybeSkipped} type parameter is
   * now {@link Skipped}.</p>
   */
  def skip(n: Int)(implicit ev: Sk =:= Unskipped): AbstractQuery[M, R, Ord, Sel, Lim, Skipped, Or]

  def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): AbstractQuery[M, R, Ord, Sel, Lim, Skipped, Or]

  /**
   * Gets the size of the query result. This should only be called on queries that do not
   * have limits or skips.
   */
  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long

  /**
   * Returns the number of distinct values returned by a query. The query must not have
   * limit or skip clauses.
   */
  def countDistinct[V](field: M => QueryField[V, M])
                      (implicit ev1: Lim =:= Unlimited,
                       ev2: Sk =:= Unskipped): Long

  /**
   * Checks if there are any records that match this query.
   */
  def exists()(implicit ev1: Lim =:= Unlimited,
               ev2: Sk =:= Unskipped): Boolean

  /**
   * Executes a function on each record value returned by a query.
   * @param f a function to be invoked on each fetched record.
   * @return nothing.
   */
  def foreach(f: R => Unit): Unit

  /**
   * Execute the query, returning all of the records that match the query.
   * @return a list containing the records that match the query
   */
  def fetch(): List[R]

  /**
   * Execute a query, returning no more than a specified number of result records. The
   * query must not have a limit clause.
   * @param limit the maximum number of records to return.
   */
  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R]

  /**
   * fetch a batch of results, and execute a function on each element of the list.
   * @param f the function to invoke on the records that match the query.
   * @return a list containing the results of invoking the function on each record.
   */
  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T]

  /**
   * Fetches the first record that matches the query. The query must not contain a "limited" clause.
   * @return an option record containing either the first result that matches the
   *     query, or None if there are no records that match.
   */
  def get()(implicit ev: Lim =:= Unlimited): Option[R]

  /**
   * Fetches the records that match the query in paginated form. The query must not contain
   * a "limit" clause.
   * @param countPerPage the number of records to be contained in each page of the result.
   */
  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited,
                                  ev2: Sk =:= Unskipped): BasePaginatedQuery[M, R]

  def noop()(implicit ev1: Sel =:= Unselected,
             ev2: Lim =:= Unlimited,
             ev3: Sk =:= Unskipped): AbstractModifyQuery[M]

  /**
   * Delete all of the records that match the query. The query must not contain any "skip",
   * "limit", or "select" clauses. Sends the delete operation to mongo, and returns - does
   * <em>not</em> wait for the delete to be finished.
   */
  def bulkDelete_!!()(implicit ev1: Sel =:= Unselected,
                      ev2: Lim =:= Unlimited,
                      ev3: Sk =:= Unskipped): Unit

  /**
   * Delete all of the recurds that match the query. The query must not contain any "skip",
   * "limit", or "select" clauses. Sends the delete operation to mongo, and waits for the
   * delete operation to complete before returning to the caller.
   */
  def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected,
                                                   ev2: Lim =:= Unlimited,
                                                   ev3: Sk =:= Unskipped): Unit

  /**
   * Finds the first record that matches the query (if any), fetches it, and then deletes it.
   * A copy of the deleted record is returned to the caller.
   */
  def findAndDeleteOne(): Option[R]

  def signature(): String

  /**
   * Return a string containing details about how the query would be executed in mongo.
   * In particular, this is useful for finding out what indexes will be used by the query.
   */
  def explain(): String

  def maxScan(max: Int): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  def comment(c: String): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

  /**
   * Adds a select clause to the query. The use of this method constrains the type
   * signature of the query to force the "Sel" field to be type "Selected".
   *
   * <p> The use of the implicit parameter here is key to how the Rogue type checking
   * mechanics work. In order to attach a "select" clause to a query, the query as it exists
   * must <em>not</em> have a select clause yet. So the implicit parameter, which carries
   * the phantom type information, must be "Unselected" before this is called. After it's called,
   * you can see that the "MaybeSelected" type parameter is changed, and is now specifically
   * bound to "Selected", rather than to a type variable.</p>
   */
  def select[F1](f: M => SelectField[F1, M])
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, F1, Ord, Selected, Lim, Sk, Or]

  def select[F1, F2](f1: M => SelectField[F1, M],
                     f2: M => SelectField[F2, M])
                    (implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or]

  def select[F1, F2, F3](f1: M => SelectField[F1, M],
                         f2: M => SelectField[F2, M],
                         f3: M => SelectField[F3, M])
                      (implicit ev: Sel =:= Unselected):
                      AbstractQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or]

  def select[F1, F2, F3, F4](f1: M => SelectField[F1, M],
                             f2: M => SelectField[F2, M],
                             f3: M => SelectField[F3, M],
                             f4: M => SelectField[F4, M])
                      (implicit ev: Sel =:= Unselected):
                      AbstractQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or]

  def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M],
                                 f2: M => SelectField[F2, M],
                                 f3: M => SelectField[F3, M],
                                 f4: M => SelectField[F4, M],
                                 f5: M => SelectField[F5, M])
                      (implicit ev: Sel =:= Unselected):
                      AbstractQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or]

  def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M],
                                     f2: M => SelectField[F2, M],
                                     f3: M => SelectField[F3, M],
                                     f4: M => SelectField[F4, M],
                                     f5: M => SelectField[F5, M],
                                     f6: M => SelectField[F6, M])
                      (implicit ev: Sel =:= Unselected):
                      AbstractQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or]

  def select[F1, F2, F3, F4, F5, F6, F7](f1: M => SelectField[F1, M],
                                         f2: M => SelectField[F2, M],
                                         f3: M => SelectField[F3, M],
                                         f4: M => SelectField[F4, M],
                                         f5: M => SelectField[F5, M],
                                         f6: M => SelectField[F6, M],
                                         f7: M => SelectField[F7, M])
                      (implicit ev: Sel =:= Unselected):
                      AbstractQuery[M, (F1, F2, F3, F4, F5, F6, F7), Ord, Selected, Lim, Sk, Or]

  def select[F1, F2, F3, F4, F5, F6, F7, F8](f1: M => SelectField[F1, M],
                                             f2: M => SelectField[F2, M],
                                             f3: M => SelectField[F3, M],
                                             f4: M => SelectField[F4, M],
                                             f5: M => SelectField[F5, M],
                                             f6: M => SelectField[F6, M],
                                             f7: M => SelectField[F7, M],
                                             f8: M => SelectField[F8, M])
                      (implicit ev: Sel =:= Unselected):
                      AbstractQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8), Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, CC](f: M => SelectField[F1, M], create: F1 => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, CC](f1: M => SelectField[F1, M],
                             f2: M => SelectField[F2, M],
                             create: (F1, F2) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M],
                                 f2: M => SelectField[F2, M],
                                 f3: M => SelectField[F3, M],
                                 create: (F1, F2, F3) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M],
                                     f2: M => SelectField[F2, M],
                                     f3: M => SelectField[F3, M],
                                     f4: M => SelectField[F4, M],
                                     create: (F1, F2, F3, F4) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M],
                                         f2: M => SelectField[F2, M],
                                         f3: M => SelectField[F3, M],
                                         f4: M => SelectField[F4, M],
                                         f5: M => SelectField[F5, M],
                                         create: (F1, F2, F3, F4, F5) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M],
                                             f2: M => SelectField[F2, M],
                                             f3: M => SelectField[F3, M],
                                             f4: M => SelectField[F4, M],
                                             f5: M => SelectField[F5, M],
                                             f6: M => SelectField[F6, M],
                                             create: (F1, F2, F3, F4, F5, F6) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, F3, F4, F5, F6, F7, CC](f1: M => SelectField[F1, M],
                                                 f2: M => SelectField[F2, M],
                                                 f3: M => SelectField[F3, M],
                                                 f4: M => SelectField[F4, M],
                                                 f5: M => SelectField[F5, M],
                                                 f6: M => SelectField[F6, M],
                                                 f7: M => SelectField[F7, M],
                                                 create: (F1, F2, F3, F4, F5, F6, F7) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, CC](f1: M => SelectField[F1, M],
                                                     f2: M => SelectField[F2, M],
                                                     f3: M => SelectField[F3, M],
                                                     f4: M => SelectField[F4, M],
                                                     f5: M => SelectField[F5, M],
                                                     f6: M => SelectField[F6, M],
                                                     f7: M => SelectField[F7, M],
                                                     f8: M => SelectField[F8, M],
                                                     create: (F1, F2, F3, F4, F5, F6, F7, F8) => CC)
                      (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  def hint(h: MongoIndex[M]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
}

case class BaseQuery[M <: MongoRecord[M], R,
                     Ord <: MaybeOrdered,
                     Sel <: MaybeSelected,
                     Lim <: MaybeLimited,
                     Sk <: MaybeSkipped,
                     Or <: MaybeHasOrClause](
    override val meta: M with MongoMetaRecord[M],
    lim: Option[Int],
    sk: Option[Int],
    maxScan: Option[Int],
    comment: Option[String],
    hint: Option[ListMap[String, Any]],
    condition: AndCondition,
    order: Option[MongoOrder],
    select: Option[MongoSelect[R, M]]) extends AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] {

  // The meta field on the MongoMetaRecord (as an instance of MongoRecord)
  // points to the master MongoMetaRecord. This is here in case you have a
  // second MongoMetaRecord pointing to the slave.
  override lazy val master = meta.meta

  private def addClause[F](clause: M => QueryClause[F],
                           expectedIndexBehavior: MaybeIndexed):
                       AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = {
    clause(meta) match {
      case cl: EmptyQueryClause[_] => new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, Or]
      case cl => {
        val newClause = cl.withExpectedIndexBehavior(expectedIndexBehavior)
        this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
      }
    }
  }

  override def where[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = Index)
  override def and[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = Index)
  override def iscan[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = IndexScan)
  override def scan[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = DocumentScan)

  private def addClauseOpt[V, F](opt: Option[V])
                       (clause: (M, V) => QueryClause[F],
                        expectedIndexBehavior: MaybeIndexed) = {
    opt match {
      case Some(v) => addClause(clause(_, v), expectedIndexBehavior)
      case None => this
    }
  }

  override def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = Index)
  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = Index)
  override def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = IndexScan)
  override def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = DocumentScan)

  override def raw(f: BasicDBObjectBuilder => Unit) = {
    val newClause = new RawQueryClause(f)
    this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
  }

  override def or(subqueries: (M with MongoMetaRecord[M] =>
                              AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _])*)
                       (implicit ev: Or =:= HasNoOrClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause] = {
    val orCondition = QueryHelpers.orConditionFromQueries(subqueries.toList.map(q => q(meta)))
    this.copy(condition = condition.copy(orCondition = Some(orCondition)))
  }

  override def orderAsc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Unordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, true)))))
  override def orderDesc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Unordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, false)))))
  override def andAsc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Ordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, true) :: order.get.terms)))
  override def andDesc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Ordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, false) :: order.get.terms)))

  override def limit(n: Int)(implicit ev: Lim =:= Unlimited): BaseQuery[M, R, Ord, Sel, Limited, Sk, Or] =
    this.copy(lim = Some(n))
  override def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): BaseQuery[M, R, Ord, Sel, Limited, Sk, Or] =
    this.copy(lim = n)
  override def skip(n: Int)(implicit ev: Sk =:= Unskipped): BaseQuery[M, R, Ord, Sel, Lim, Skipped, Or] =
    this.copy(sk = Some(n))
  override def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): BaseQuery[M, R, Ord, Sel, Lim, Skipped, Or] =
    this.copy(sk = n)

  private[rogue] def parseDBObject(dbo: DBObject): R = select match {
    case Some(MongoSelect(Nil, transformer)) =>
      // A MongoSelect clause exists, but has empty fields. Return null.
      // This is used for .exists(), where we just want to check the number
      // of returned results is > 0.
      transformer(null)
    case Some(MongoSelect(fields, transformer)) =>
      val inst = fields.head.field.owner
      def setInstanceFieldFromDbo(field: Field[_, M]) = {
        inst.fieldByName(field.name) match {
          case Full(fld) => fld.setFromAny(dbo.get(field.name))
          case _ => {
            val splitName = field.name.split('.').toList
            Box.!!(splitName.foldLeft(dbo: Object)((obj: Object, fieldName: String) => {
              obj match {
                case dbl: BasicBSONList =>
                  (for {
                    index <- 0 to dbl.size - 1
                    val item: DBObject = dbl.get(index).asInstanceOf[DBObject]
                  } yield item.get(fieldName)).toList
                case dbo: DBObject =>
                  dbo.get(fieldName)
              }
            }))
          }
        }
      }
      setInstanceFieldFromDbo(inst.fieldByName("_id").open_!)
      transformer(fields.map(fld => fld(setInstanceFieldFromDbo(fld.field))))
    case None => meta.fromDBObject(dbo).asInstanceOf[R]
  }

  private def drainBuffer[A, B](from: ListBuffer[A],
                                to: ListBuffer[B],
                                f: List[A] => List[B], size: Int): Unit = {
    if (from.size >= size) {
      to ++= f(from.toList)
      from.clear
    }
  }

  override def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("count", this)(meta.count(_))

  override def countDistinct[V](field: M => QueryField[V, M])
                       (implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("countDistinct", this)(meta.countDistinct(field(meta).field.name, _))

  override def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean =
    this.copy(select = Some(MongoSelect[Null, M](Nil, _ => null))).limit(1).fetch().size > 0

  override def foreach(f: R => Unit): Unit =
    QueryExecutor.query("find", this, None)(dbo => f(parseDBObject(dbo)))

  override def fetch(): List[R] = {
    val rv = new ListBuffer[R]
    QueryExecutor.query("find", this, None)(dbo => rv += parseDBObject(dbo))
    rv.toList
  }
  override def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] =
    this.limit(limit).fetch()
  override def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = {
    val rv = new ListBuffer[T]
    val buf = new ListBuffer[R]

    QueryExecutor.query("find", this, Some(batchSize)) { dbo =>
      buf += parseDBObject(dbo)
      drainBuffer(buf, rv, f, batchSize)
    }
    drainBuffer(buf, rv, f, 1)

    rv.toList
  }

  override def get()(implicit ev: Lim =:= Unlimited): Option[R] =
    fetch(1).headOption

  override def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    new BasePaginatedQuery(this.copy(), countPerPage)
  }

  override def noop()(implicit ev1: Sel =:= Unselected,
                      ev2: Lim =:= Unlimited,
                      ev3: Sk =:= Unskipped) = BaseModifyQuery(this, MongoModify(Nil))

  // Always do modifications against master (not meta, which could point to slave)
  override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected,
                               ev2: Lim =:= Unlimited,
                               ev3: Sk =:= Unskipped): Unit =
    QueryExecutor.condition("remove", this)(master.bulkDelete_!!(_))

  override def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected,
                                                            ev2: Lim =:= Unlimited,
                                                            ev3: Sk =:= Unskipped): Unit =
    QueryExecutor.condition("remove", this) { qry =>
      MongoDB.useCollection(master.mongoIdentifier, master.collectionName) { coll =>
        coll.remove(qry, concern)
      }
    }

  override def findAndDeleteOne(): Option[R] = {
    val mod = BaseFindAndModifyQuery(this, MongoModify(Nil))
    QueryExecutor.findAndModify(mod, returnNew=false, upsert=false, remove=true)(this.parseDBObject _)
  }

  override def toString: String =
    MongoBuilder.buildQueryString("find", this)

  override def signature(): String =
    MongoBuilder.buildSignature(this)

  override def explain(): String =
    QueryExecutor.explain("find", this)

  override def maxScan(max: Int): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(maxScan = Some(max))

  override def comment(c: String): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(comment = Some(c))

  override def hint(index: MongoIndex[M]) = this.copy(hint = Some(index.asListMap))

  override def select[F1](f: M => SelectField[F1, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, F1, Ord, Selected, Lim, Sk, Or] = {
    selectCase(f, (f: F1) => f)
  }

  override def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, (f1: F1, f2: F2) => (f1, f2))
  }

  override def select[F1, F2, F3](f1: M => SelectField[F1, M],
                                  f2: M => SelectField[F2, M],
                                  f3: M => SelectField[F3, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, (f1: F1, f2: F2, f3: F3) => (f1, f2, f3))
  }

  override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M],
                                      f2: M => SelectField[F2, M],
                                      f3: M => SelectField[F3, M],
                                      f4: M => SelectField[F4, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, (f1: F1, f2: F2, f3: F3, f4: F4) => (f1, f2, f3, f4))
  }

  override def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M],
                                          f2: M => SelectField[F2, M],
                                          f3: M => SelectField[F3, M],
                                          f4: M => SelectField[F4, M],
                                          f5: M => SelectField[F5, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5) => (f1, f2, f3, f4, f5))
  }

  override def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M],
                                              f2: M => SelectField[F2, M],
                                              f3: M => SelectField[F3, M],
                                              f4: M => SelectField[F4, M],
                                              f5: M => SelectField[F5, M],
                                              f6: M => SelectField[F6, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6) => (f1, f2, f3, f4, f5, f6))
  }

  override def select[F1, F2, F3, F4, F5, F6, F7](f1: M => SelectField[F1, M],
                                                  f2: M => SelectField[F2, M],
                                                  f3: M => SelectField[F3, M],
                                                  f4: M => SelectField[F4, M],
                                                  f5: M => SelectField[F5, M],
                                                  f6: M => SelectField[F6, M],
                                                  f7: M => SelectField[F7, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5, F6, F7), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7,
               (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7) => (f1, f2, f3, f4, f5, f6, f7))
  }

  override def select[F1, F2, F3, F4, F5, F6, F7, F8](f1: M => SelectField[F1, M],
                                                      f2: M => SelectField[F2, M],
                                                      f3: M => SelectField[F3, M],
                                                      f4: M => SelectField[F4, M],
                                                      f5: M => SelectField[F5, M],
                                                      f6: M => SelectField[F6, M],
                                                      f7: M => SelectField[F7, M],
                                                      f8: M => SelectField[F8, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, f8,
               (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7, f8: F8) => (f1, f2, f3, f4, f5, f6, f7, f8))
  }

  def selectCase[F1, CC](f: M => SelectField[F1, M],
                         create: F1 => CC)(implicit ev: Sel =:= Unselected):
                       BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f(inst))
    val transformer = (xs: List[_]) => create(xs.head.asInstanceOf[F1])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, CC](f1: M => SelectField[F1, M],
                             f2: M => SelectField[F2, M],
                             create: (F1, F2) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M],
                                 f2: M => SelectField[F2, M],
                                 f3: M => SelectField[F3, M],
                                 create: (F1, F2, F3) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M],
                                     f2: M => SelectField[F2, M],
                                     f3: M => SelectField[F3, M],
                                     f4: M => SelectField[F4, M],
                                     create: (F1, F2, F3, F4) => CC)
                         (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M],
                                         f2: M => SelectField[F2, M],
                                         f3: M => SelectField[F3, M],
                                         f4: M => SelectField[F4, M],
                                         f5: M => SelectField[F5, M],
                                         create: (F1, F2, F3, F4, F5) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4],
                                              xs(4).asInstanceOf[F5])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M],
                                             f2: M => SelectField[F2, M],
                                             f3: M => SelectField[F3, M],
                                             f4: M => SelectField[F4, M],
                                             f5: M => SelectField[F5, M],
                                             f6: M => SelectField[F6, M],
                                             create: (F1, F2, F3, F4, F5, F6) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4],
                                              xs(4).asInstanceOf[F5],
                                              xs(5).asInstanceOf[F6])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, CC](f1: M => SelectField[F1, M],
                                                 f2: M => SelectField[F2, M],
                                                 f3: M => SelectField[F3, M],
                                                 f4: M => SelectField[F4, M],
                                                 f5: M => SelectField[F5, M],
                                                 f6: M => SelectField[F6, M],
                                                 f7: M => SelectField[F7, M],
                                                 create: (F1, F2, F3, F4, F5, F6, F7) => CC)
                       (   implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4],
                                              xs(4).asInstanceOf[F5],
                                              xs(5).asInstanceOf[F6],
                                              xs(6).asInstanceOf[F7])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, CC](f1: M => SelectField[F1, M],
                                                     f2: M => SelectField[F2, M],
                                                     f3: M => SelectField[F3, M],
                                                     f4: M => SelectField[F4, M],
                                                     f5: M => SelectField[F5, M],
                                                     f6: M => SelectField[F6, M],
                                                     f7: M => SelectField[F7, M],
                                                     f8: M => SelectField[F8, M],
                                                     create: (F1, F2, F3, F4, F5, F6, F7, F8) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst), f8(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4],
                                              xs(4).asInstanceOf[F5],
                                              xs(5).asInstanceOf[F6],
                                              xs(6).asInstanceOf[F7],
                                              xs(7).asInstanceOf[F8])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }
}

class BaseEmptyQuery[M <: MongoRecord[M], R,
                     Ord <: MaybeOrdered,
                     Sel <: MaybeSelected,
                     Lim <: MaybeLimited,
                     Sk <: MaybeSkipped,
                     Or <: MaybeHasOrClause] extends AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] {
  override lazy val meta = throw new Exception("tried to read meta field of an EmptyQuery")

  override lazy val master = throw new Exception("tried to read master field of an EmptyQuery")

  override def where[F](clause: M => QueryClause[F]) = this

  override def and[F](clause: M => QueryClause[F]) = this

  override def iscan[F](clause: M => QueryClause[F]) = this

  override def scan[F](clause: M => QueryClause[F]) = this

  override def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

  override def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

  override def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

  override def raw(f: BasicDBObjectBuilder => Unit) = this

  override def or(subqueries: (M with MongoMetaRecord[M] =>
                               AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _])*)
                       (implicit ev: Or =:= HasNoOrClause) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause]

  override def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) =
    new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

  override def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) =
    new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

  override def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) =
    new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

  override def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) =
    new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

  override def limit(n: Int)(implicit ev: Lim =:= Unlimited) =
    new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk, Or]

  override def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited) =
    new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk, Or]

  override def skip(n: Int)(implicit ev: Sk =:= Unskipped) =
    new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped, Or]

  override def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped) =
    new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped, Or]

  override def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0

  override def countDistinct[V](field: M => QueryField[V, M])
                       (implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0
  override def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean = false

  override def foreach(f: R => Unit): Unit = ()

  override def fetch(): List[R] = Nil

  override def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] = Nil

  override def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = Nil

  override def get()(implicit ev: Lim =:= Unlimited): Option[R] = None

  override def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    val emptyQuery = new BaseEmptyQuery[M, R, Ord, Sel, Unlimited, Unskipped, Or]
    new BasePaginatedQuery(emptyQuery, countPerPage)
  }

  override def noop()(implicit ev1: Sel =:= Unselected,
                      ev2: Lim =:= Unlimited,
                      ev3: Sk =:= Unskipped) =
    new EmptyModifyQuery[M]

  override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected,
                               ev2: Lim =:= Unlimited,
                               ev3: Sk =:= Unskipped): Unit = ()

  override def blockingBulkDelete_!!(concern: WriteConcern)
                       (implicit ev1: Sel =:= Unselected,
                        ev2: Lim =:= Unlimited,
                        ev3: Sk =:= Unskipped): Unit = ()

  override def findAndDeleteOne(): Option[R] = None

  override def toString = "empty query"

  override def signature = "empty query"

  override def explain = "{}"

  override def maxScan(max: Int) = this

  override def comment(c: String) = this

  override def hint(index: MongoIndex[M]) = this

  override def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, F1, Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2](f1: M => SelectField[F1, M],
                              f2: M => SelectField[F2, M])
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2, F3](f1: M => SelectField[F1, M],
                                  f2: M => SelectField[F2, M],
                                  f3: M => SelectField[F3, M])
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M],
                                      f2: M => SelectField[F2, M],
                                      f3: M => SelectField[F3, M],
                                      f4: M => SelectField[F4, M])
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M],
                                          f2: M => SelectField[F2, M],
                                          f3: M => SelectField[F3, M],
                                          f4: M => SelectField[F4, M],
                                          f5: M => SelectField[F5, M])
                       (implicit ev: Sel =:= Unselected) =
     new BaseEmptyQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M],
                                              f2: M => SelectField[F2, M],
                                              f3: M => SelectField[F3, M],
                                              f4: M => SelectField[F4, M],
                                              f5: M => SelectField[F5, M],
                                              f6: M => SelectField[F6, M])
                       (implicit ev: Sel =:= Unselected) =
     new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2, F3, F4, F5, F6, F7](f1: M => SelectField[F1, M],
                                                  f2: M => SelectField[F2, M],
                                                  f3: M => SelectField[F3, M],
                                                  f4: M => SelectField[F4, M],
                                                  f5: M => SelectField[F5, M],
                                                  f6: M => SelectField[F6, M],
                                                  f7: M => SelectField[F7, M])
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6, F7), Ord, Selected, Lim, Sk, Or]

  override def select[F1, F2, F3, F4, F5, F6, F7, F8](f1: M => SelectField[F1, M],
                                                      f2: M => SelectField[F2, M],
                                                      f3: M => SelectField[F3, M],
                                                      f4: M => SelectField[F4, M],
                                                      f5: M => SelectField[F5, M],
                                                      f6: M => SelectField[F6, M],
                                                      f7: M => SelectField[F7, M],
                                                      f8: M => SelectField[F8, M])
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8), Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, CC](f: M => SelectField[F1, M],
                                  create: F1 => CC)(implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, CC](f1: M => SelectField[F1, M],
                                      f2: M => SelectField[F2, M],
                                      create: (F1, F2) => CC)
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M],
                                          f2: M => SelectField[F2, M],
                                          f3: M => SelectField[F3, M],
                                          create: (F1, F2, F3) => CC)
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M],
                                              f2: M => SelectField[F2, M],
                                              f3: M => SelectField[F3, M],
                                              f4: M => SelectField[F4, M],
                                              create: (F1, F2, F3, F4) => CC)
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M],
                                                  f2: M => SelectField[F2, M],
                                                  f3: M => SelectField[F3, M],
                                                  f4: M => SelectField[F4, M],
                                                  f5: M => SelectField[F5, M],
                                                  create: (F1, F2, F3, F4, F5) => CC)
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M],
                                                      f2: M => SelectField[F2, M],
                                                      f3: M => SelectField[F3, M],
                                                      f4: M => SelectField[F4, M],
                                                      f5: M => SelectField[F5, M],
                                                      f6: M => SelectField[F6, M],
                                                      create: (F1, F2, F3, F4, F5, F6) => CC)
                       (implicit ev: Sel =:= Unselected) =
     new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, F3, F4, F5, F6, F7, CC](f1: M => SelectField[F1, M],
                                                          f2: M => SelectField[F2, M],
                                                          f3: M => SelectField[F3, M],
                                                          f4: M => SelectField[F4, M],
                                                          f5: M => SelectField[F5, M],
                                                          f6: M => SelectField[F6, M],
                                                          f7: M => SelectField[F7, M],
                                                          create: (F1, F2, F3, F4, F5, F6, F7) => CC)
                       (implicit ev: Sel =:= Unselected) =
      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

  override def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, CC](f1: M => SelectField[F1, M],
                                                              f2: M => SelectField[F2, M],
                                                              f3: M => SelectField[F3, M],
                                                              f4: M => SelectField[F4, M],
                                                              f5: M => SelectField[F5, M],
                                                              f6: M => SelectField[F6, M],
                                                              f7: M => SelectField[F7, M],
                                                              f8: M => SelectField[F8, M],
                                                              create: (F1, F2, F3, F4, F5, F6, F7, F8) => CC)
     (implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]
}

// *******************************************************
// *** Modify Queries
// *******************************************************

trait AbstractModifyQuery[M <: MongoRecord[M]] {
  def modify[F](clause: M => ModifyClause[F]): AbstractModifyQuery[M]

  def and[F](clause: M => ModifyClause[F]): AbstractModifyQuery[M]

  def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractModifyQuery[M]

  def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractModifyQuery[M]

  def updateMulti(): Unit

  def updateOne(): Unit

  def upsertOne(): Unit

  // These must be overloads and not default arguments because Scala does not allow a caller to omit parentheses
  // when there are default arguments. As many existing uses of these methods omit the parentheses, these overloads
  // are necessary to avoid breaking callers.
  def updateMulti(writeConcern: WriteConcern): Unit

  def updateOne(writeConcern: WriteConcern): Unit

  def upsertOne(writeConcern: WriteConcern): Unit
}

case class BaseModifyQuery[M <: MongoRecord[M]](query: BaseQuery[M, _, _ <: MaybeOrdered,
                                                                 _ <: MaybeSelected,
                                                                 _ <: MaybeLimited,
                                                                 _ <: MaybeSkipped,
                                                                 _ <: MaybeHasOrClause],
                                                mod: MongoModify) extends AbstractModifyQuery[M] {

  private def addClause[F](clause: M => ModifyClause[F]) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  override def modify[F](clause: M => ModifyClause[F]) = addClause(clause)
  override def and[F](clause: M => ModifyClause[F]) = addClause(clause)

  private def addClauseOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = {
    opt match {
      case Some(v) => addClause(clause(_, v))
      case None => this
    }
  }

  override def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  // These methods always do modifications against master (not query.meta, which could point to a slave).
  override def updateMulti(): Unit =
      QueryExecutor.modify(this, upsert = false, multi = true, writeConcern = None)

  override def updateOne(): Unit =
      QueryExecutor.modify(this, upsert = false, multi = false, writeConcern = None)

  override def upsertOne(): Unit =
      QueryExecutor.modify(this, upsert = true, multi = false, writeConcern = None)

  override def updateMulti(writeConcern: WriteConcern): Unit =
      QueryExecutor.modify(this, upsert = false, multi = true, writeConcern = Some(writeConcern))

  override def updateOne(writeConcern: WriteConcern): Unit =
      QueryExecutor.modify(this, upsert = false, multi = false, writeConcern = Some(writeConcern))

  override def upsertOne(writeConcern: WriteConcern): Unit =
      QueryExecutor.modify(this, upsert = true, multi = false, writeConcern = Some(writeConcern))

  override def toString = MongoBuilder.buildModifyString(this)
}

class EmptyModifyQuery[M <: MongoRecord[M]] extends AbstractModifyQuery[M] {
  override def modify[F](clause: M => ModifyClause[F]) = this

  override def and[F](clause: M => ModifyClause[F]) = this

  override def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

  override def updateMulti(): Unit = ()

  override def updateOne(): Unit = ()

  override def upsertOne(): Unit = ()

  def updateMulti(writeConcern: WriteConcern): Unit = ()

  def updateOne(writeConcern: WriteConcern): Unit = ()

  def upsertOne(writeConcern: WriteConcern): Unit = ()

  override def toString = "empty modify query"
}

// *******************************************************
// *** FindAndModify Queries
// *******************************************************

trait AbstractFindAndModifyQuery[M <: MongoRecord[M], R] {
  def findAndModify[F](clause: M => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

  def and[F](clause: M => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

  def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

  def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

  def updateOne(returnNew: Boolean = false): Option[R]

  def upsertOne(returnNew: Boolean = false): Option[R]
}

case class BaseFindAndModifyQuery[M <: MongoRecord[M], R](query: BaseQuery[M, R, _ <: MaybeOrdered,
                                                                           _ <: MaybeSelected,
                                                                           _ <: MaybeLimited,
                                                                           _ <: MaybeSkipped,
                                                                           _ <: MaybeHasOrClause],
                                                          mod: MongoModify) extends AbstractFindAndModifyQuery[M, R] {

  private def addClause[F](clause: M => ModifyClause[F]) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  override def findAndModify[F](clause: M => ModifyClause[F]) = addClause(clause)

  override def and[F](clause: M => ModifyClause[F]) = addClause(clause)

  private def addClauseOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = {
    opt match {
      case Some(v) => addClause(clause(_, v))
      case None => this
    }
  }

  override def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  // Always do modifications against master (not query.meta, which could point to slave)
  override def updateOne(returnNew: Boolean = false): Option[R] = {
    QueryExecutor.findAndModify(this, returnNew, upsert=false, remove=false)(query.parseDBObject _)
  }
  override def upsertOne(returnNew: Boolean = false): Option[R] = {
    QueryExecutor.findAndModify(this, returnNew, upsert=true, remove=false)(query.parseDBObject _)
  }

  override def toString = MongoBuilder.buildFindAndModifyString(this, false, false, false)
}

class EmptyFindAndModifyQuery[M <: MongoRecord[M], R] extends AbstractFindAndModifyQuery[M, R] {
  override def findAndModify[F](clause: M => ModifyClause[F]) = this

  override def and[F](clause: M => ModifyClause[F]) = this

  override def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

  override def updateOne(returnNew: Boolean = false): Option[Nothing] = None

  override def upsertOne(returnNew: Boolean = false): Option[Nothing] = None

  override def toString = "empty findAndModify query"
}

class BasePaginatedQuery[M <: MongoRecord[M], R]
        (q: AbstractQuery[M, R, _, _, Unlimited, Unskipped, _],
         val countPerPage: Int, val pageNum: Int = 1) {
  def copy() = new BasePaginatedQuery(q, countPerPage, pageNum)

  def setPage(p: Int) = if (p == pageNum) this else new BasePaginatedQuery(q, countPerPage, p)

  def setCountPerPage(c: Int) = if (c == countPerPage) this else new BasePaginatedQuery(q, c, pageNum)

  lazy val countAll: Long = q.count

  def fetch(): List[R] = q.skip(countPerPage * (pageNum - 1)).limit(countPerPage).fetch()

  def numPages = math.ceil(countAll.toDouble / countPerPage.toDouble).toInt max 1
}
