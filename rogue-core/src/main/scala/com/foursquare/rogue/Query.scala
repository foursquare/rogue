// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.index.MongoIndex
import com.foursquare.rogue.MongoHelpers.{
    AndCondition, MongoBuilder, MongoModify, MongoOrder, MongoSelect}
import com.mongodb.{BasicDBObjectBuilder, DBObject, ReadPreference, WriteConcern}
import org.bson.types.BasicBSONList
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
 * @tparam M the record type being queried.
 * @tparam R
 * @tparam State a phantom type which defines the state of the builder.
 */
case class Query[M, R, +State](
    meta: M,
    collectionName: String,
    lim: Option[Int],
    sk: Option[Int],
    maxScan: Option[Int],
    comment: Option[String],
    hint: Option[ListMap[String, Any]],
    condition: AndCondition,
    order: Option[MongoOrder],
    select: Option[MongoSelect[M, R]],
    readPreference: Option[ReadPreference]
) {

  private def addClause[F](clause: M => QueryClause[F],
                           expectedIndexBehavior: MaybeIndexed):
                       Query[M, R, State] = {
    val newClause = clause(meta)
    newClause.expectedIndexBehavior = expectedIndexBehavior
    this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
  }

  /**
   * Adds a where clause to a query.
   */
  def where[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = Index)

  /**
   * Adds another and-connected clause to the query.
   */
  def and[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = Index)

  /**
   * Adds an iscan clause to a query.
   */
  def iscan[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = IndexScan)

  /**
   * Adds a scan clause to a query.
   */
  def scan[F](clause: M => QueryClause[F]) =
    addClause(clause, expectedIndexBehavior = DocumentScan)

  /**
   * Adds an eqs clause specifying the shard key.
   */
  def withShardKey[F, S2](clause: M => QueryClause[F] with ShardKeyClause)
                         (implicit ev: AddShardAware[State, S2, _]): Query[M, R, S2] = {
    addClause(clause, expectedIndexBehavior = DocumentScan).asInstanceOf[Query[M, R, S2]]
  }

  def allShards[S2](implicit ev: AddShardAware[State, _, S2]): Query[M, R, S2] = {
    this.asInstanceOf[Query[M, R, S2]]
  }

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

  def raw(f: BasicDBObjectBuilder => Unit): Query[M, R, State] = {
    val newClause = new RawQueryClause(f)
    newClause.expectedIndexBehavior = DocumentScan
    this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
  }

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
  def or[S2](subqueries: (Query[M, R, Ordered with Selected with Limited with Skipped with HasNoOrClause] => Query[M, R, _])*)
            (implicit ev: AddOrClause[State, S2]): Query[M, R, S2] = {
    val queryBuilder =
      this.copy[M, R, Ordered with Selected with Limited with Skipped with HasNoOrClause](
        lim = None,
        sk = None,
        maxScan = None,
        comment = None,
        hint = None,
        condition = AndCondition(Nil, None),
        order = None,
        select = None,
        readPreference = None)
    val queries = subqueries.toList.map(q => q(queryBuilder))
    val orCondition = QueryHelpers.orConditionFromQueries(queries)
    this.copy(condition = condition.copy(orCondition = Some(orCondition)))
  }

  /**
   * Like "or", this uses the Rogue phantom-type/implicit parameter mechanics. To call this
   * method, the query must <em>not</em> yet have an ordering clause attached. This is captured
   * by the implicit parameter being constrained to be "Unordered". After this is called, the
   * type signature of the returned query is updated so that the "MaybeOrdered" type parameter is
   * now Ordered.
   */
  def orderAsc[S2](field: M => AbstractQueryField[_, _, _, M])
                     (implicit ev: AddOrder[State, S2]): Query[M, R, S2] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, true)))))

  def orderDesc[S2](field: M => AbstractQueryField[_, _, _, M])
                      (implicit ev: AddOrder[State, S2]): Query[M, R, S2] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, false)))))

  def andAsc(field: M => AbstractQueryField[_, _, _, M])
               (implicit ev: State <:< Ordered): Query[M, R, State] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, true) :: order.get.terms)))

  def andDesc(field: M => AbstractQueryField[_, _, _, M])
                (implicit ev: State <:< Ordered): Query[M, R, State] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, false) :: order.get.terms)))

  /**
   * Natural ordering.
   * TODO: doesn't make sense in conjunction with ordering on any other fields. enforce w/ phantom types?
   */
  def orderNaturalAsc[V, S2](implicit ev: AddOrder[State, S2]): Query[M, R, S2] =
    this.copy(order = Some(MongoOrder(List(("$natural", true)))))

  def orderNaturalDesc[V, S2](implicit ev: AddOrder[State, S2]): Query[M, R, S2] =
    this.copy(order = Some(MongoOrder(List(("$natural", false)))))

  /**
   * Places a limit on the size of the returned result.
   *
   * <p> Like "or", this uses the Rogue phantom-type/implicit parameter mechanics. To call this
   * method, the query must <em>not</em> yet have a limit clause attached. This is captured
   * by the implicit parameter being constrained to be "Unlimited". After this is called, the
   * type signature of the returned query is updated so that the "MaybeLimited" type parameter is
   * now Limited.</p>
   */
  def limit[S2](n: Int)(implicit ev: AddLimit[State, S2]): Query[M, R, S2] =
    this.copy(lim = Some(n))

  def limitOpt[S2](n: Option[Int])(implicit ev: AddLimit[State, S2]): Query[M, R, S2] =
    this.copy(lim = n)

  /**
   * Adds a skip to the query.
   *
   * <p> Like {@link or}, this uses the Rogue phantom-type/implicit parameter mechanics. To call this
   * method, the query must <em>not</em> yet have a skip clause attached. This is captured
   * by the implicit parameter being constrained to be {@link Unskipped}. After this is called, the
   * type signature of the returned query is updated so that the {@link MaybeSkipped} type parameter is
   * now {@link Skipped}.</p>
   */
  def skip[S2](n: Int)(implicit ev: AddSkip[State, S2]): Query[M, R, S2] =
    this.copy(sk = Some(n))

  def skipOpt[S2](n: Option[Int])(implicit ev: AddSkip[State, S2]): Query[M, R, S2] =
    this.copy(sk = n)

  def noop()
          (implicit ev1: Required[State, Unselected with Unlimited with Unskipped],
           ev2: ShardingOk[M, State]): ModifyQuery[M, State] = {
    ModifyQuery(this, MongoModify(Nil))
  }

  def modify(clause: M => ModifyClause)
            (implicit ev1: Required[State, Unselected with Unlimited with Unskipped],
             ev2: ShardingOk[M, State]): ModifyQuery[M, State] = {
    ModifyQuery(this, MongoModify(Nil)).modify(clause)
  }
  def modifyOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause)
                  (implicit ev1: Required[State, Unselected with Unlimited with Unskipped],
                   ev2: ShardingOk[M, State]): ModifyQuery[M, State] = {
    ModifyQuery(this, MongoModify(Nil)).modifyOpt(opt)(clause)
  }

  def findAndModify[F](clause: M => ModifyClause)
                      (implicit ev1: Required[State, Unlimited with Unskipped],
                      ev2: RequireShardKey[M, State]): FindAndModifyQuery[M, R] = {
    FindAndModifyQuery[M, R](this, MongoModify(Nil)).findAndModify(clause)
  }

  def findAndModifyOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause)
                      (implicit ev1: Required[State, Unlimited with Unskipped],
                       ev2: RequireShardKey[M, State]): FindAndModifyQuery[M, R] = {
    FindAndModifyQuery[M, R](this, MongoModify(Nil)).findAndModifyOpt(opt)(clause)
  }

  override def toString: String =
    MongoBuilder.buildQueryString("find", collectionName, this)

  def asDBObject: DBObject =
    MongoBuilder.buildCondition(this.condition)

  def signature(): String =
    MongoBuilder.buildSignature(collectionName, this)

  def maxScan(max: Int): Query[M, R, State] = this.copy(maxScan = Some(max))

  def comment(c: String): Query[M, R, State] = this.copy(comment = Some(c))

  /**
   * Set a flag to indicate whether this query should hit primaries or secondaries.
   * This only really makes sense if you're using replica sets. If this field is
   * unspecified, rogue will leave the option untouched, so you'll use
   * secondaries or not depending on how you configure the mongo java driver.
   * Also, this only works if you're doing a query -- findAndModify, updates,
   * and deletes always go to the primaries.
   *
   * For more info, see
   * http://www.mongodb.org/display/DOCS/slaveOk
   */
  def setReadPreference(r: ReadPreference): Query[M, R, State] = this.copy(readPreference = Some(r))

  def hint(index: MongoIndex[M]): Query[M, R, State] = this.copy(hint = Some(index.asListMap))

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

  /*

  // Code to generate the select() and selectCase() methods:

  def select(n: Int) = {
    val typeParams = (1 to n).map(i => "F%d".format(i)).mkString(", ")
    val arguments = (1 to n).map(i => "f%d: M => SelectField[F%d, M]".format(i, i)).mkString(", ")
    val outParams = if (n == 1) "_, S2" else "S2, _"
    val resultType = if (n == 1) typeParams else "(%s)".format(typeParams)
    val vars = (1 to n).map(i => "f%d".format(i)).mkString(", ")
    val fnArgs = (1 to n).map(i => "f%d: F%d".format(i, i)).mkString(", ")
    val fnResult = if (n == 1) vars else "(%s)".format(vars)
    val code = """
    def select[%s, S2](%s)
          (implicit ev: AddSelect[State, %s]): BaseQuery[M, %s, S2] = {
      selectCase(%s, (%s) => %s)
    }"""
    code.format(typeParams, arguments, outParams, resultType, vars, fnArgs, fnResult)
  }

  def selectCase(n: Int) = {
    val typeParams = (1 to n).map(i => "F%d".format(i)).mkString(", ")
    val arguments = (1 to n).map(i => "f%d: M => SelectField[F%d, M]".format(i, i)).mkString(", ")
    val outParams = if (n == 1) "_, S2" else "S2, _"
    val resultType = if (n == 1) typeParams else "(%s)".format(typeParams)
    val instCalls = (1 to n).map(i => "f%d(inst)".format(i)).mkString(", ")
    val createArgs = (1 to n).map(i => "xs(%d).asInstanceOf[F%d]".format(i-1, i)).mkString(", ")
    val code = """
    def selectCase[%s, CC, S2](%s,
          create: %s => CC)(implicit ev: AddSelect[State, %s]): BaseQuery[M, CC, S2] = {
      val inst = meta
      val fields = List(%s)
      val transformer = (xs: List[_]) => create(%s)
      this.copy(select = Some(MongoSelect(fields, transformer)))
    }"""
    code.format(typeParams, arguments, resultType, outParams, instCalls, createArgs)
  }

  def generate(n:	Int) = {
    (1 to n).foreach(i => println(select(i)))
    (1 to n).foreach(i => println(selectCase(i)))
  }
   */

  def select[F1, S2](f1: M => SelectField[F1, M])
        (implicit ev: AddSelect[State, _, S2]): Query[M, F1, S2] = {
    selectCase(f1, (f1: F1) => f1)
  }

  def select[F1, F2, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2), S2] = {
    selectCase(f1, f2, (f1: F1, f2: F2) => (f1, f2))
  }

  def select[F1, F2, F3, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3), S2] = {
    selectCase(f1, f2, f3, (f1: F1, f2: F2, f3: F3) => (f1, f2, f3))
  }

  def select[F1, F2, F3, F4, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4), S2] = {
    selectCase(f1, f2, f3, f4, (f1: F1, f2: F2, f3: F3, f4: F4) => (f1, f2, f3, f4))
  }

  def select[F1, F2, F3, F4, F5, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4, F5), S2] = {
    selectCase(f1, f2, f3, f4, f5, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5) => (f1, f2, f3, f4, f5))
  }

  def select[F1, F2, F3, F4, F5, F6, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4, F5, F6), S2] = {
    selectCase(f1, f2, f3, f4, f5, f6, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6) => (f1, f2, f3, f4, f5, f6))
  }

  def select[F1, F2, F3, F4, F5, F6, F7, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4, F5, F6, F7), S2] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7) => (f1, f2, f3, f4, f5, f6, f7))
  }

  def select[F1, F2, F3, F4, F5, F6, F7, F8, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M], f8: M => SelectField[F8, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4, F5, F6, F7, F8), S2] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, f8, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7, f8: F8) => (f1, f2, f3, f4, f5, f6, f7, f8))
  }

  def select[F1, F2, F3, F4, F5, F6, F7, F8, F9, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M], f8: M => SelectField[F8, M], f9: M => SelectField[F9, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9), S2] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, f8, f9, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7, f8: F8, f9: F9) => (f1, f2, f3, f4, f5, f6, f7, f8, f9))
  }

  def select[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M], f8: M => SelectField[F8, M], f9: M => SelectField[F9, M], f10: M => SelectField[F10, M])
        (implicit ev: AddSelect[State, S2, _]): Query[M, (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10), S2] = {
    selectCase(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6, f7: F7, f8: F8, f9: F9, f10: F10) => (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10))
  }

  def selectCase[F1, CC, S2](f1: M => SelectField[F1, M],
        create: F1 => CC)(implicit ev: AddSelect[State, _, S2]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M],
        create: (F1, F2) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M],
        create: (F1, F2, F3) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M],
        create: (F1, F2, F3, F4) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M],
        create: (F1, F2, F3, F4, F5) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M],
        create: (F1, F2, F3, F4, F5, F6) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M],
        create: (F1, F2, F3, F4, F5, F6, F7) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6], xs(6).asInstanceOf[F7])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M], f8: M => SelectField[F8, M],
        create: (F1, F2, F3, F4, F5, F6, F7, F8) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst), f8(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6], xs(6).asInstanceOf[F7], xs(7).asInstanceOf[F8])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M], f8: M => SelectField[F8, M], f9: M => SelectField[F9, M],
        create: (F1, F2, F3, F4, F5, F6, F7, F8, F9) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst), f8(inst), f9(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6], xs(6).asInstanceOf[F7], xs(7).asInstanceOf[F8], xs(8).asInstanceOf[F9])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, CC, S2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], f7: M => SelectField[F7, M], f8: M => SelectField[F8, M], f9: M => SelectField[F9, M], f10: M => SelectField[F10, M],
        create: (F1, F2, F3, F4, F5, F6, F7, F8, F9, F10) => CC)(implicit ev: AddSelect[State, S2, _]): Query[M, CC, S2] = {
    val inst = meta
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst), f7(inst), f8(inst), f9(inst), f10(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6], xs(6).asInstanceOf[F7], xs(7).asInstanceOf[F8], xs(8).asInstanceOf[F9], xs(9).asInstanceOf[F10])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }
}

// *******************************************************
// *** Modify Queries
// *******************************************************

case class ModifyQuery[M, +State](
  	query: Query[M, _, State],
    mod: MongoModify
) {

  private def addClause(clause: M => ModifyClause) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  def modify(clause: M => ModifyClause) = addClause(clause)
  def and(clause: M => ModifyClause) = addClause(clause)

  private def addClauseOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause) = {
    opt match {
      case Some(v) => addClause(clause(_, v))
      case None => this
    }
  }

  def modifyOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause) =
      addClauseOpt(opt)(clause)

  def andOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause) =
      addClauseOpt(opt)(clause)

  override def toString: String = MongoBuilder.buildModifyString(query.collectionName, this)

  def asDBObject = (this.query.asDBObject, MongoBuilder.buildModify(this.mod))
}

// *******************************************************
// *** FindAndModify Queries
// *******************************************************

case class FindAndModifyQuery[M, R](
    query: Query[M, R, _],
    mod: MongoModify
) {

  private def addClause(clause: M => ModifyClause) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  def findAndModify[F](clause: M => ModifyClause) = addClause(clause)
  def and[F](clause: M => ModifyClause) = addClause(clause)

  private def addClauseOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause) = {
    opt match {
      case Some(v) => addClause(clause(_, v))
      case None => this
    }
  }

  def findAndModifyOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause) =
      addClauseOpt(opt)(clause)

  def andOpt[V](opt: Option[V])(clause: (M, V) => ModifyClause) =
      addClauseOpt(opt)(clause)

  override def toString: String = MongoBuilder.buildFindAndModifyString(query.collectionName, this, false, false, false)
}
