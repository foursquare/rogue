// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers.{
    AndCondition, MongoBuilder, MongoModify, MongoOrder, MongoSelect}
import com.mongodb.{BasicDBObjectBuilder, DBObject, WriteConcern}
import net.liftweb.common.{Box, Full}
import org.bson.types.BasicBSONList
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

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
// trait AbstractQuery[M <: MongoRecord[M], R,
//                     Ord <: MaybeOrdered,
//                     Sel <: MaybeSelected,
//                     Lim <: MaybeLimited,
//                     Sk <: MaybeSkipped,
//                     Or <: MaybeHasOrClause] {

//   def meta: M with MongoMetaRecord[M]
//   def master: MongoMetaRecord[M]

//   /**
//    * Adds a where clause to a query.
//    */
//   def where[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   /**
//    * Adds another and-connected clause to the query.
//    */
//   def and[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   /**
//    * Adds a scan clause to a query.
//    */
//   def scan[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   /**
//    * Adds an iscan clause to a query.
//    */
//   def iscan[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
//   def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
//   def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
//   def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   def raw(f: BasicDBObjectBuilder => Unit): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   /**
//    * Chains an "or" subquery to the current query.
//    *
//    * <p> The use of the implicit parameter here is key to how the Rogue type checking
//    * mechanics work. In order to attach an "or" clause to a query, the query as it exists
//    * must <em>not</em> yet have an or-clause. So the implicit parameter, which carries
//    * the phantom type information, must be "HasNoOrClause" before this is called. After it's called,
//    * you can see that the "MaybeHasOrClause" type parameter is changed, and is now specifically
//    * bound to "HasOrClause", rather than to a type variable.</p>
//    */
//   def or(subqueries: (M with MongoMetaRecord[M] =>
//                       AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _])*)
//                       (implicit ev: Or =:= HasNoOrClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause]

//    /**
//     * Like "or", this uses the Rogue phantom-type/implicit parameter mechanics. To call this
//     * method, the query must <em>not</em> yet have an ordering clause attached. This is captured
//     * by the implicit parameter being constrained to be "Unordered". After this is called, the
//     * type signature of the returned query is updated so that the "MaybeOrdered" type parameter is
//     * now Ordered.
//     */
//   def orderAsc[V](field: M => QueryField[V, M])
//                       (implicit ev: Ord =:= Unordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]
//   def orderDesc[V](field: M => QueryField[V, M])
//                       (implicit ev: Ord =:= Unordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]
//   def andAsc[V](field: M => QueryField[V, M])
//                       (implicit ev: Ord =:= Ordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]
//   def andDesc[V](field: M => QueryField[V, M])
//                       (implicit ev: Ord =:= Ordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or]

//   /**
//    * Places a limit on the size of the returned result.
//    *
//    * <p> Like "or", this uses the Rogue phantom-type/implicit parameter mechanics. To call this
//    * method, the query must <em>not</em> yet have a limit clause attached. This is captured
//    * by the implicit parameter being constrained to be "Unlimited". After this is called, the
//    * type signature of the returned query is updated so that the "MaybeLimited" type parameter is
//    * now Limited.</p>
//    */
//   def limit(n: Int)(implicit ev: Lim =:= Unlimited): AbstractQuery[M, R, Ord, Sel, Limited, Sk, Or]

//   def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): AbstractQuery[M, R, Ord, Sel, Limited, Sk, Or]

//   /**
//    * Adds a skip to the query.
//    *
//    * <p> Like {@link or}, this uses the Rogue phantom-type/implicit parameter mechanics. To call this
//    * method, the query must <em>not</em> yet have a skip clause attached. This is captured
//    * by the implicit parameter being constrained to be {@link Unskipped}. After this is called, the
//    * type signature of the returned query is updated so that the {@link MaybeSkipped} type parameter is
//    * now {@link Skipped}.</p>
//    */
//   def skip(n: Int)(implicit ev: Sk =:= Unskipped): AbstractQuery[M, R, Ord, Sel, Lim, Skipped, Or]

//   def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): AbstractQuery[M, R, Ord, Sel, Lim, Skipped, Or]

//   /**
//    * Gets the size of the query result. This should only be called on queries that do not
//    * have limits or skips.
//    */
//   def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long

//   /**
//    * Returns the number of distinct values returned by a query. The query must not have
//    * limit or skip clauses.
//    */
//   def countDistinct[V](field: M => QueryField[V, M])
//                       (implicit ev1: Lim =:= Unlimited,
//                        ev2: Sk =:= Unskipped): Long

//   /**
//    * Checks if there are any records that match this query.
//    */
//   def exists()(implicit ev1: Lim =:= Unlimited,
//                ev2: Sk =:= Unskipped): Boolean

//   /**
//    * Executes a function on each record value returned by a query.
//    * @param f a function to be invoked on each fetched record.
//    * @return nothing.
//    */
//   def foreach(f: R => Unit): Unit

//   /**
//    * Execute the query, returning all of the records that match the query.
//    * @return a list containing the records that match the query
//    */
//   def fetch(): List[R]

//   /**
//    * Execute a query, returning no more than a specified number of result records. The
//    * query must not have a limit clause.
//    * @param limit the maximum number of records to return.
//    */
//   def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R]

//   /**
//    * fetch a batch of results, and execute a function on each element of the list.
//    * @param f the function to invoke on the records that match the query.
//    * @return a list containing the results of invoking the function on each record.
//    */
//   def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T]

//   /**
//    * Fetches the first record that matches the query. The query must not contain a "limited" clause.
//    * @return an option record containing either the first result that matches the
//    *     query, or None if there are no records that match.
//    */
//   def get()(implicit ev: Lim =:= Unlimited): Option[R]

//   /**
//    * Fetches the records that match the query in paginated form. The query must not contain
//    * a "limit" clause.
//    * @param countPerPage the number of records to be contained in each page of the result.
//    */
//   def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited,
//                                   ev2: Sk =:= Unskipped): BasePaginatedQuery[M, R]

//   def noop()(implicit ev1: Sel =:= Unselected,
//              ev2: Lim =:= Unlimited,
//              ev3: Sk =:= Unskipped): AbstractModifyQuery[M]

//   /**
//    * Delete all of the records that match the query. The query must not contain any "skip",
//    * "limit", or "select" clauses. Sends the delete operation to mongo, and returns - does
//    * <em>not</em> wait for the delete to be finished.
//    */
//   def bulkDelete_!!()(implicit ev1: Sel =:= Unselected,
//                       ev2: Lim =:= Unlimited,
//                       ev3: Sk =:= Unskipped): Unit

//   /**
//    * Delete all of the recurds that match the query. The query must not contain any "skip",
//    * "limit", or "select" clauses. Sends the delete operation to mongo, and waits for the
//    * delete operation to complete before returning to the caller.
//    */
//   def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected,
//                                                    ev2: Lim =:= Unlimited,
//                                                    ev3: Sk =:= Unskipped): Unit

//   /**
//    * Finds the first record that matches the query (if any), fetches it, and then deletes it.
//    * A copy of the deleted record is returned to the caller.
//    */
//   def findAndDeleteOne(): Option[R]

//   def signature(): String

//   /**
//    * Return a string containing details about how the query would be executed in mongo.
//    * In particular, this is useful for finding out what indexes will be used by the query.
//    */
//   def explain(): String

//   def maxScan(max: Int): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   def comment(c: String): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   /**
//    * Set a flag to indicate whether this query may hit secondaries. This only
//    * really makes sense if you're using replica sets. If this field is
//    * unspecified, rogue will leave the option untouched, so you'll use
//    * secondaries or not depending on how you configure the mongo java driver.
//    * Also, this only works if you're doing a query -- findAndModify, updates,
//    * and deletes always go to the primaries.
//    *
//    * For more info, see
//    * http://www.mongodb.org/display/DOCS/Querying#Querying-slaveOk%28QueryingSecondaries%29.
//    */
//   def setSlaveOk(b: Boolean): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]

//   /**
//    * Adds a select clause to the query. The use of this method constrains the type
//    * signature of the query to force the "Sel" field to be type "Selected".
//    *
//    * <p> The use of the implicit parameter here is key to how the Rogue type checking
//    * mechanics work. In order to attach a "select" clause to a query, the query as it exists
//    * must <em>not</em> have a select clause yet. So the implicit parameter, which carries
//    * the phantom type information, must be "Unselected" before this is called. After it's called,
//    * you can see that the "MaybeSelected" type parameter is changed, and is now specifically
//    * bound to "Selected", rather than to a type variable.</p>
//    */
//   def select[F1](f: M => SelectField[F1, M])
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, F1, Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2](f1: M => SelectField[F1, M],
//                      f2: M => SelectField[F2, M])
//                     (implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3](f1: M => SelectField[F1, M],
//                          f2: M => SelectField[F2, M],
//                          f3: M => SelectField[F3, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4](f1: M => SelectField[F1, M],
//                              f2: M => SelectField[F2, M],
//                              f3: M => SelectField[F3, M],
//                              f4: M => SelectField[F4, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M],
//                                  f2: M => SelectField[F2, M],
//                                  f3: M => SelectField[F3, M],
//                                  f4: M => SelectField[F4, M],
//                                  f5: M => SelectField[F5, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M],
//                                      f2: M => SelectField[F2, M],
//                                      f3: M => SelectField[F3, M],
//                                      f4: M => SelectField[F4, M],
//                                      f5: M => SelectField[F5, M],
//                                      f6: M => SelectField[F6, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4, F5, F6, F7](f1: M => SelectField[F1, M],
//                                          f2: M => SelectField[F2, M],
//                                          f3: M => SelectField[F3, M],
//                                          f4: M => SelectField[F4, M],
//                                          f5: M => SelectField[F5, M],
//                                          f6: M => SelectField[F6, M],
//                                          f7: M => SelectField[F7, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4, F5, F6, F7), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4, F5, F6, F7, F8](f1: M => SelectField[F1, M],
//                                              f2: M => SelectField[F2, M],
//                                              f3: M => SelectField[F3, M],
//                                              f4: M => SelectField[F4, M],
//                                              f5: M => SelectField[F5, M],
//                                              f6: M => SelectField[F6, M],
//                                              f7: M => SelectField[F7, M],
//                                              f8: M => SelectField[F8, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4, F5, F6, F7, F8, F9](f1: M => SelectField[F1, M],
//                                                 f2: M => SelectField[F2, M],
//                                                 f3: M => SelectField[F3, M],
//                                                 f4: M => SelectField[F4, M],
//                                                 f5: M => SelectField[F5, M],
//                                                 f6: M => SelectField[F6, M],
//                                                 f7: M => SelectField[F7, M],
//                                                 f8: M => SelectField[F8, M],
//                                                 f9: M => SelectField[F9, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9), Ord, Selected, Lim, Sk, Or]

//   def select[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10](f1: M => SelectField[F1, M],
//                                                       f2: M => SelectField[F2, M],
//                                                       f3: M => SelectField[F3, M],
//                                                       f4: M => SelectField[F4, M],
//                                                       f5: M => SelectField[F5, M],
//                                                       f6: M => SelectField[F6, M],
//                                                       f7: M => SelectField[F7, M],
//                                                       f8: M => SelectField[F8, M],
//                                                       f9: M => SelectField[F9, M],
//                                                       f10: M => SelectField[F10, M])
//                       (implicit ev: Sel =:= Unselected):
//                       AbstractQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10), Ord, Selected, Lim, Sk, Or]


//   def selectCase[F1, CC](f: M => SelectField[F1, M], create: F1 => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, CC](f1: M => SelectField[F1, M],
//                              f2: M => SelectField[F2, M],
//                              create: (F1, F2) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M],
//                                  f2: M => SelectField[F2, M],
//                                  f3: M => SelectField[F3, M],
//                                  create: (F1, F2, F3) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M],
//                                      f2: M => SelectField[F2, M],
//                                      f3: M => SelectField[F3, M],
//                                      f4: M => SelectField[F4, M],
//                                      create: (F1, F2, F3, F4) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M],
//                                          f2: M => SelectField[F2, M],
//                                          f3: M => SelectField[F3, M],
//                                          f4: M => SelectField[F4, M],
//                                          f5: M => SelectField[F5, M],
//                                          create: (F1, F2, F3, F4, F5) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M],
//                                              f2: M => SelectField[F2, M],
//                                              f3: M => SelectField[F3, M],
//                                              f4: M => SelectField[F4, M],
//                                              f5: M => SelectField[F5, M],
//                                              f6: M => SelectField[F6, M],
//                                              create: (F1, F2, F3, F4, F5, F6) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, F5, F6, F7, CC](f1: M => SelectField[F1, M],
//                                                  f2: M => SelectField[F2, M],
//                                                  f3: M => SelectField[F3, M],
//                                                  f4: M => SelectField[F4, M],
//                                                  f5: M => SelectField[F5, M],
//                                                  f6: M => SelectField[F6, M],
//                                                  f7: M => SelectField[F7, M],
//                                                  create: (F1, F2, F3, F4, F5, F6, F7) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, CC](f1: M => SelectField[F1, M],
//                                                      f2: M => SelectField[F2, M],
//                                                      f3: M => SelectField[F3, M],
//                                                      f4: M => SelectField[F4, M],
//                                                      f5: M => SelectField[F5, M],
//                                                      f6: M => SelectField[F6, M],
//                                                      f7: M => SelectField[F7, M],
//                                                      f8: M => SelectField[F8, M],
//                                                      create: (F1, F2, F3, F4, F5, F6, F7, F8) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, CC](f1: M => SelectField[F1, M],
//                                                          f2: M => SelectField[F2, M],
//                                                          f3: M => SelectField[F3, M],
//                                                          f4: M => SelectField[F4, M],
//                                                          f5: M => SelectField[F5, M],
//                                                          f6: M => SelectField[F6, M],
//                                                          f7: M => SelectField[F7, M],
//                                                          f8: M => SelectField[F8, M],
//                                                          f9: M => SelectField[F9, M],
//                                                      create: (F1, F2, F3, F4, F5, F6, F7, F8, F9) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, CC](f1: M => SelectField[F1, M],
//                                                               f2: M => SelectField[F2, M],
//                                                               f3: M => SelectField[F3, M],
//                                                               f4: M => SelectField[F4, M],
//                                                               f5: M => SelectField[F5, M],
//                                                               f6: M => SelectField[F6, M],
//                                                               f7: M => SelectField[F7, M],
//                                                               f8: M => SelectField[F8, M],
//                                                               f9: M => SelectField[F9, M],
//                                                               f10: M => SelectField[F10, M],
//                                                      create: (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10) => CC)
//                       (implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or]
//   def hint(h: MongoIndex[M]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or]
// }

case class BaseQuery[
    M,
    R,
    Ord <: MaybeOrdered,
    Sel <: MaybeSelected,
    Lim <: MaybeLimited,
    Sk <: MaybeSkipped,
    Or <: MaybeHasOrClause
](
    meta: M,
    collectionName: String,
    lim: Option[Int],
    sk: Option[Int],
    maxScan: Option[Int],
    comment: Option[String],
    hint: Option[ListMap[String, Any]],
    condition: AndCondition,
    order: Option[MongoOrder],
    select: Option[MongoSelect[R]],
    slaveOk: Option[Boolean]
) {

  // The meta field on the MongoMetaRecord (as an instance of MongoRecord)
  // points to the master MongoMetaRecord. This is here in case you have a
  // second MongoMetaRecord pointing to the slave.
  // lazy val master = meta.meta

  private def addClause[F](clause: M => QueryClause[F],
                           expectedIndexBehavior: MaybeIndexed):
                       AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = {
    clause(meta) match {
      // case cl: EmptyQueryClause[_] => new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, Or]
      case cl => {
        // val newClause = cl.withExpectedIndexBehavior(expectedIndexBehavior)
        this.copy(condition = condition.copy(clauses = cl :: condition.clauses))
      }
    }
  }

  def where[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = Index)
  def and[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = Index)
  def iscan[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = IndexScan)
  def scan[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = DocumentScan)

  private def addClauseOpt[V, F](opt: Option[V])
                       (clause: (M, V) => QueryClause[F],
                        expectedIndexBehavior: MaybeIndexed) = {
    opt match {
      case Some(v) => addClause(clause(_, v), expectedIndexBehavior)
      case None => this
    }
  }

  def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = Index)
  def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = Index)
  def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = IndexScan)
  def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) =
    addClauseOpt(opt)(clause, expectedIndexBehavior = DocumentScan)

  def raw(f: BasicDBObjectBuilder => Unit) = {
    val newClause = new RawQueryClause(f)
    this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
  }

  def or(subqueries: (AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] => AbstractQuery[M, R, Unordered, Unselected, Unlimited, Unskipped, _])*)
                       (implicit ev: Or =:= HasNoOrClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause] = {
    val queryBuilder =
      this.copy[M, R, Ord, Sel, Lim, Sk, Or](
        lim = None,
        sk = None,
        maxScan = None,
        comment = None,
        hint = None,
        condition = AndCondition(Nil, None),
        order = None,
        select = None,
        slaveOk = None)
    val queries = subqueries.toList.map(q => q(queryBuilder))
    val orCondition = QueryHelpers.orConditionFromQueries(queries)
    this.copy(condition = condition.copy(orCondition = Some(orCondition)))
  }

  def orderAsc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Unordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, true)))))
  def orderDesc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Unordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, false)))))
  def andAsc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Ordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, true) :: order.get.terms)))
  def andDesc[V](field: M => QueryField[V, M])
                       (implicit ev: Ord =:= Ordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, false) :: order.get.terms)))

  def limit(n: Int)(implicit ev: Lim =:= Unlimited): BaseQuery[M, R, Ord, Sel, Limited, Sk, Or] =
    this.copy(lim = Some(n))
  def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): BaseQuery[M, R, Ord, Sel, Limited, Sk, Or] =
    this.copy(lim = n)
  def skip(n: Int)(implicit ev: Sk =:= Unskipped): BaseQuery[M, R, Ord, Sel, Lim, Skipped, Or] =
    this.copy(sk = Some(n))
  def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): BaseQuery[M, R, Ord, Sel, Lim, Skipped, Or] =
    this.copy(sk = n)

  def noop()(implicit ev1: Sel =:= Unselected,
                      ev2: Lim =:= Unlimited,
                      ev3: Sk =:= Unskipped): BaseModifyQuery[M] = BaseModifyQuery(this, MongoModify(Nil)) // TODO: Does this work?

  override def toString: String =
    MongoBuilder.buildQueryString("find", collectionName, this)

  def signature(): String =
    MongoBuilder.buildSignature(collectionName, this)

  def maxScan(max: Int): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(maxScan = Some(max))

  def comment(c: String): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(comment = Some(c))

  def setSlaveOk(b: Boolean): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(slaveOk = Some(b))

  def hint(index: MongoIndex[M]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(hint = Some(index.asListMap))

  def select[F1](f: M => SelectField[F1, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, F1, Ord, Selected, Lim, Sk, Or] = {
    selectCase(f, (f: F1) => f)
  }

  def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, (f1: F1, f2: F2) => (f1, f2))
  }

  def select[F1, F2, F3](f1: M => SelectField[F1, M],
                                  f2: M => SelectField[F2, M],
                                  f3: M => SelectField[F3, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, (f1: F1, f2: F2, f3: F3) => (f1, f2, f3))
  }

  def select[F1, F2, F3, F4](f1: M => SelectField[F1, M],
                                      f2: M => SelectField[F2, M],
                                      f3: M => SelectField[F3, M],
                                      f4: M => SelectField[F4, M])
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, (f1: F1, f2: F2, f3: F3, f4: F4) => (f1, f2, f3, f4))
  }

  def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M],
                                          f2: M => SelectField[F2, M],
                                          f3: M => SelectField[F3, M],
                                          f4: M => SelectField[F4, M],
                                          f5: M => SelectField[F5, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5) => (f1, f2, f3, f4, f5))
  }

  def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M],
                                              f2: M => SelectField[F2, M],
                                              f3: M => SelectField[F3, M],
                                              f4: M => SelectField[F4, M],
                                              f5: M => SelectField[F5, M],
                                              f6: M => SelectField[F6, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6) => (f1, f2, f3, f4, f5, f6))
  }

  def select[F1, F2, F3, F4, F5, F6, F7](f1: M => SelectField[F1, M],
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

  def select[F1, F2, F3, F4, F5, F6, F7, F8](f1: M => SelectField[F1, M],
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

  def select[F1, F2, F3, F4, F5, F6, F7, F8, F9](f1: M => SelectField[F1, M],
                                                          f2: M => SelectField[F2, M],
                                                          f3: M => SelectField[F3, M],
                                                          f4: M => SelectField[F4, M],
                                                          f5: M => SelectField[F5, M],
                                                          f6: M => SelectField[F6, M],
                                                          f7: M => SelectField[F7, M],
                                                          f8: M => SelectField[F8, M],
                                                          f9: M => SelectField[F9, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, f8, f9,
               (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7, f8: F8, f9: F9) =>
               (f1, f2, f3, f4, f5, f6, f7, f8, f9))
  }

  def select[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10](f1: M => SelectField[F1, M],
                                                               f2: M => SelectField[F2, M],
                                                               f3: M => SelectField[F3, M],
                                                               f4: M => SelectField[F4, M],
                                                               f5: M => SelectField[F5, M],
                                                               f6: M => SelectField[F6, M],
                                                               f7: M => SelectField[F7, M],
                                                               f8: M => SelectField[F8, M],
                                                               f9: M => SelectField[F9, M],
                                                               f10: M => SelectField[F10, M])
                       (implicit ev: Sel =:= Unselected):
                       BaseQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10,
               (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7, f8: F8, f9: F9, f10: F10) =>
               (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10))
  }

  def selectCase[F1, CC](f: M => SelectField[F1, M],
                         create: F1 => CC)(implicit ev: Sel =:= Unselected):
                       BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta
    val fields = List(f(inst))
    val transformer = (xs: List[_]) => create(xs.head.asInstanceOf[F1])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, CC](f1: M => SelectField[F1, M],
                             f2: M => SelectField[F2, M],
                             create: (F1, F2) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M],
                                 f2: M => SelectField[F2, M],
                                 f3: M => SelectField[F3, M],
                                 create: (F1, F2, F3) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta
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
    val inst = meta
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
    val inst = meta
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
    val inst = meta
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
    val inst = meta
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
    val inst = meta
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

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, CC](f1: M => SelectField[F1, M],
                                                     f2: M => SelectField[F2, M],
                                                     f3: M => SelectField[F3, M],
                                                     f4: M => SelectField[F4, M],
                                                     f5: M => SelectField[F5, M],
                                                     f6: M => SelectField[F6, M],
                                                     f7: M => SelectField[F7, M],
                                                     f8: M => SelectField[F8, M],
                                                     f9: M => SelectField[F9, M],
                                                     create: (F1, F2, F3, F4, F5, F6, F7, F8, F9) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst), f8(inst), f9(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4],
                                              xs(4).asInstanceOf[F5],
                                              xs(5).asInstanceOf[F6],
                                              xs(6).asInstanceOf[F7],
                                              xs(7).asInstanceOf[F8],
                                              xs(8).asInstanceOf[F9])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, CC](f1: M => SelectField[F1, M],
                                                     f2: M => SelectField[F2, M],
                                                     f3: M => SelectField[F3, M],
                                                     f4: M => SelectField[F4, M],
                                                     f5: M => SelectField[F5, M],
                                                     f6: M => SelectField[F6, M],
                                                     f7: M => SelectField[F7, M],
                                                     f8: M => SelectField[F8, M],
                                                     f9: M => SelectField[F9, M],
                                                     f10: M => SelectField[F10, M],
                                                     create: (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10) => CC)
                       (implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst),
                      f6(inst), f7(inst), f8(inst), f9(inst), f10(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1],
                                              xs(1).asInstanceOf[F2],
                                              xs(2).asInstanceOf[F3],
                                              xs(3).asInstanceOf[F4],
                                              xs(4).asInstanceOf[F5],
                                              xs(5).asInstanceOf[F6],
                                              xs(6).asInstanceOf[F7],
                                              xs(7).asInstanceOf[F8],
                                              xs(8).asInstanceOf[F9],
                                              xs(9).asInstanceOf[F10])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }
}

// class BaseEmptyQuery[M <: MongoRecord[M], R,
//                      Ord <: MaybeOrdered,
//                      Sel <: MaybeSelected,
//                      Lim <: MaybeLimited,
//                      Sk <: MaybeSkipped,
//                      Or <: MaybeHasOrClause] extends AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or] {
//   override lazy val meta = throw new Exception("tried to read meta field of an EmptyQuery")

//   override lazy val master = throw new Exception("tried to read master field of an EmptyQuery")

//   override def where[F](clause: M => QueryClause[F]) = this

//   override def and[F](clause: M => QueryClause[F]) = this

//   override def iscan[F](clause: M => QueryClause[F]) = this

//   override def scan[F](clause: M => QueryClause[F]) = this

//   override def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

//   override def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

//   override def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

//   override def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this

//   override def raw(f: BasicDBObjectBuilder => Unit) = this

//   override def or(subqueries: (M with MongoMetaRecord[M] =>
//                                AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _])*)
//                        (implicit ev: Or =:= HasNoOrClause) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause]

//   override def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) =
//     new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

//   override def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) =
//     new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

//   override def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) =
//     new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

//   override def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) =
//     new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or]

//   override def limit(n: Int)(implicit ev: Lim =:= Unlimited) =
//     new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk, Or]

//   override def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited) =
//     new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk, Or]

//   override def skip(n: Int)(implicit ev: Sk =:= Unskipped) =
//     new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped, Or]

//   override def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped) =
//     new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped, Or]

//   override def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0

//   override def countDistinct[V](field: M => QueryField[V, M])
//                        (implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0
//   override def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean = false

//   override def foreach(f: R => Unit): Unit = ()

//   override def fetch(): List[R] = Nil

//   override def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] = Nil

//   override def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = Nil

//   override def get()(implicit ev: Lim =:= Unlimited): Option[R] = None

//   override def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
//     val emptyQuery = new BaseEmptyQuery[M, R, Ord, Sel, Unlimited, Unskipped, Or]
//     new BasePaginatedQuery(emptyQuery, countPerPage)
//   }

//   override def noop()(implicit ev1: Sel =:= Unselected,
//                       ev2: Lim =:= Unlimited,
//                       ev3: Sk =:= Unskipped) =
//     new EmptyModifyQuery[M]

//   override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected,
//                                ev2: Lim =:= Unlimited,
//                                ev3: Sk =:= Unskipped): Unit = ()

//   override def blockingBulkDelete_!!(concern: WriteConcern)
//                        (implicit ev1: Sel =:= Unselected,
//                         ev2: Lim =:= Unlimited,
//                         ev3: Sk =:= Unskipped): Unit = ()

//   override def findAndDeleteOne(): Option[R] = None

//   override def toString = "empty query"

//   override def signature = "empty query"

//   override def explain = "{}"

//   override def maxScan(max: Int) = this

//   override def comment(c: String) = this

//   override def setSlaveOk(b: Boolean) = this

//   override def hint(index: MongoIndex[M]) = this

//   override def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, F1, Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2](f1: M => SelectField[F1, M],
//                               f2: M => SelectField[F2, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3](f1: M => SelectField[F1, M],
//                                   f2: M => SelectField[F2, M],
//                                   f3: M => SelectField[F3, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M],
//                                       f2: M => SelectField[F2, M],
//                                       f3: M => SelectField[F3, M],
//                                       f4: M => SelectField[F4, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M],
//                                           f2: M => SelectField[F2, M],
//                                           f3: M => SelectField[F3, M],
//                                           f4: M => SelectField[F4, M],
//                                           f5: M => SelectField[F5, M])
//                        (implicit ev: Sel =:= Unselected) =
//      new BaseEmptyQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M],
//                                               f2: M => SelectField[F2, M],
//                                               f3: M => SelectField[F3, M],
//                                               f4: M => SelectField[F4, M],
//                                               f5: M => SelectField[F5, M],
//                                               f6: M => SelectField[F6, M])
//                        (implicit ev: Sel =:= Unselected) =
//      new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4, F5, F6, F7](f1: M => SelectField[F1, M],
//                                                   f2: M => SelectField[F2, M],
//                                                   f3: M => SelectField[F3, M],
//                                                   f4: M => SelectField[F4, M],
//                                                   f5: M => SelectField[F5, M],
//                                                   f6: M => SelectField[F6, M],
//                                                   f7: M => SelectField[F7, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6, F7), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4, F5, F6, F7, F8](f1: M => SelectField[F1, M],
//                                                       f2: M => SelectField[F2, M],
//                                                       f3: M => SelectField[F3, M],
//                                                       f4: M => SelectField[F4, M],
//                                                       f5: M => SelectField[F5, M],
//                                                       f6: M => SelectField[F6, M],
//                                                       f7: M => SelectField[F7, M],
//                                                       f8: M => SelectField[F8, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4, F5, F6, F7, F8, F9](f1: M => SelectField[F1, M],
//                                                           f2: M => SelectField[F2, M],
//                                                           f3: M => SelectField[F3, M],
//                                                           f4: M => SelectField[F4, M],
//                                                           f5: M => SelectField[F5, M],
//                                                           f6: M => SelectField[F6, M],
//                                                           f7: M => SelectField[F7, M],
//                                                           f8: M => SelectField[F8, M],
//                                                           f9: M => SelectField[F9, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9), Ord, Selected, Lim, Sk, Or]

//   override def select[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10](f1: M => SelectField[F1, M],
//                                                                f2: M => SelectField[F2, M],
//                                                                f3: M => SelectField[F3, M],
//                                                                f4: M => SelectField[F4, M],
//                                                                f5: M => SelectField[F5, M],
//                                                                f6: M => SelectField[F6, M],
//                                                                f7: M => SelectField[F7, M],
//                                                                f8: M => SelectField[F8, M],
//                                                                f9: M => SelectField[F9, M],
//                                                                f10: M => SelectField[F10, M])
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10), Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, CC](f: M => SelectField[F1, M],
//                                   create: F1 => CC)(implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, CC](f1: M => SelectField[F1, M],
//                                       f2: M => SelectField[F2, M],
//                                       create: (F1, F2) => CC)
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M],
//                                           f2: M => SelectField[F2, M],
//                                           f3: M => SelectField[F3, M],
//                                           create: (F1, F2, F3) => CC)
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M],
//                                               f2: M => SelectField[F2, M],
//                                               f3: M => SelectField[F3, M],
//                                               f4: M => SelectField[F4, M],
//                                               create: (F1, F2, F3, F4) => CC)
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M],
//                                                   f2: M => SelectField[F2, M],
//                                                   f3: M => SelectField[F3, M],
//                                                   f4: M => SelectField[F4, M],
//                                                   f5: M => SelectField[F5, M],
//                                                   create: (F1, F2, F3, F4, F5) => CC)
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M],
//                                                       f2: M => SelectField[F2, M],
//                                                       f3: M => SelectField[F3, M],
//                                                       f4: M => SelectField[F4, M],
//                                                       f5: M => SelectField[F5, M],
//                                                       f6: M => SelectField[F6, M],
//                                                       create: (F1, F2, F3, F4, F5, F6) => CC)
//                        (implicit ev: Sel =:= Unselected) =
//      new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, F5, F6, F7, CC](f1: M => SelectField[F1, M],
//                                                           f2: M => SelectField[F2, M],
//                                                           f3: M => SelectField[F3, M],
//                                                           f4: M => SelectField[F4, M],
//                                                           f5: M => SelectField[F5, M],
//                                                           f6: M => SelectField[F6, M],
//                                                           f7: M => SelectField[F7, M],
//                                                           create: (F1, F2, F3, F4, F5, F6, F7) => CC)
//                        (implicit ev: Sel =:= Unselected) =
//       new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, CC](f1: M => SelectField[F1, M],
//                                                               f2: M => SelectField[F2, M],
//                                                               f3: M => SelectField[F3, M],
//                                                               f4: M => SelectField[F4, M],
//                                                               f5: M => SelectField[F5, M],
//                                                               f6: M => SelectField[F6, M],
//                                                               f7: M => SelectField[F7, M],
//                                                               f8: M => SelectField[F8, M],
//                                                               create: (F1, F2, F3, F4, F5, F6, F7, F8) => CC)
//      (implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, CC](f1: M => SelectField[F1, M],
//                                                               f2: M => SelectField[F2, M],
//                                                               f3: M => SelectField[F3, M],
//                                                               f4: M => SelectField[F4, M],
//                                                               f5: M => SelectField[F5, M],
//                                                               f6: M => SelectField[F6, M],
//                                                               f7: M => SelectField[F7, M],
//                                                               f8: M => SelectField[F8, M],
//                                                               f9: M => SelectField[F9, M],
//                                                               create: (F1, F2, F3, F4, F5, F6, F7, F8, F9) => CC)
//      (implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

//   override def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, CC](f1: M => SelectField[F1, M],
//                                                               f2: M => SelectField[F2, M],
//                                                               f3: M => SelectField[F3, M],
//                                                               f4: M => SelectField[F4, M],
//                                                               f5: M => SelectField[F5, M],
//                                                               f6: M => SelectField[F6, M],
//                                                               f7: M => SelectField[F7, M],
//                                                               f8: M => SelectField[F8, M],
//                                                               f9: M => SelectField[F9, M],
//                                                               f10: M => SelectField[F10, M],
//                                                               create: (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10) => CC)
//      (implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or]

// }

// *******************************************************
// *** Modify Queries
// *******************************************************

// trait AbstractModifyQuery[M <: MongoRecord[M]] {
//   def modify[F](clause: M => ModifyClause[F]): AbstractModifyQuery[M]

//   def and[F](clause: M => ModifyClause[F]): AbstractModifyQuery[M]

//   def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractModifyQuery[M]

//   def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractModifyQuery[M]

//   def updateMulti(): Unit

//   def updateOne(): Unit

//   def upsertOne(): Unit

//   // These must be overloads and not default arguments because Scala does not allow a caller to omit parentheses
//   // when there are default arguments. As many existing uses of these methods omit the parentheses, these overloads
//   // are necessary to avoid breaking callers.
//   def updateMulti(writeConcern: WriteConcern): Unit

//   def updateOne(writeConcern: WriteConcern): Unit

//   def upsertOne(writeConcern: WriteConcern): Unit
// }

case class BaseModifyQuery[M](
  	query: BaseQuery[
        M,
        _,
        _ <: MaybeOrdered,
        _ <: MaybeSelected,
        _ <: MaybeLimited,
        _ <: MaybeSkipped,
        _ <: MaybeHasOrClause],
    mod: MongoModify
) {

  private def addClause[F](clause: M => ModifyClause[F]) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  def modify[F](clause: M => ModifyClause[F]) = addClause(clause)
  def and[F](clause: M => ModifyClause[F]) = addClause(clause)

  private def addClauseOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = {
    opt match {
      case Some(v) => addClause(clause(_, v))
      case None => this
    }
  }

  def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  override def toString: String = MongoBuilder.buildModifyString(query.collectionName, this)
}

// class EmptyModifyQuery[M <: MongoRecord[M]] extends AbstractModifyQuery[M] {
//   override def modify[F](clause: M => ModifyClause[F]) = this

//   override def and[F](clause: M => ModifyClause[F]) = this

//   override def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

//   override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

//   override def updateMulti(): Unit = ()

//   override def updateOne(): Unit = ()

//   override def upsertOne(): Unit = ()

//   def updateMulti(writeConcern: WriteConcern): Unit = ()

//   def updateOne(writeConcern: WriteConcern): Unit = ()

//   def upsertOne(writeConcern: WriteConcern): Unit = ()

//   override def toString = "empty modify query"
// }

// *******************************************************
// *** FindAndModify Queries
// *******************************************************

// trait AbstractFindAndModifyQuery[M <: MongoRecord[M], R] {
//   def findAndModify[F](clause: M => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

//   def and[F](clause: M => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

//   def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

//   def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

//   def updateOne(returnNew: Boolean = false): Option[R]

//   def upsertOne(returnNew: Boolean = false): Option[R]
// }

case class BaseFindAndModifyQuery[M, R](
    query: BaseQuery[
        M,
        R,
        _ <: MaybeOrdered,
        _ <: MaybeSelected,
        _ <: MaybeLimited,
        _ <: MaybeSkipped,
        _ <: MaybeHasOrClause],
    mod: MongoModify
) {

  private def addClause[F](clause: M => ModifyClause[F]) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  def findAndModify[F](clause: M => ModifyClause[F]) = addClause(clause)

  def and[F](clause: M => ModifyClause[F]) = addClause(clause)

  private def addClauseOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = {
    opt match {
      case Some(v) => addClause(clause(_, v))
      case None => this
    }
  }

  def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) =
      addClauseOpt(opt)(clause)

  override def toString: String = MongoBuilder.buildFindAndModifyString(query.collectionName, this, false, false, false)
}

// class EmptyFindAndModifyQuery[M <: MongoRecord[M], R] extends AbstractFindAndModifyQuery[M, R] {
//   override def findAndModify[F](clause: M => ModifyClause[F]) = this

//   override def and[F](clause: M => ModifyClause[F]) = this

//   override def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

//   override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = this

//   override def updateOne(returnNew: Boolean = false): Option[Nothing] = None

//   override def upsertOne(returnNew: Boolean = false): Option[Nothing] = None

//   override def toString = "empty findAndModify query"
// }
