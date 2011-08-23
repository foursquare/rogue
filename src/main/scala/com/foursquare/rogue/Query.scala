// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import collection.immutable.List._
import com.foursquare.rogue.MongoHelpers._
import com.mongodb.{BasicDBObjectBuilder, DBObject, WriteConcern}
import net.liftweb.record.Field
import net.liftweb.common.{Box, Full}
import net.liftweb.mongodb.MongoDB
import net.liftweb.mongodb.record._
import net.liftweb.record.Field
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

/////////////////////////////////////////////////////////////////////////////
// Phantom types
/////////////////////////////////////////////////////////////////////////////

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

abstract sealed class MaybeHasJsWhereClause
abstract sealed class HasJsWhereClause extends MaybeHasJsWhereClause
abstract sealed class HasNoJsWhereClause extends MaybeHasJsWhereClause

/////////////////////////////////////////////////////////////////////////////
// Builders
/////////////////////////////////////////////////////////////////////////////


trait AbstractQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped, Or <: MaybeHasOrClause, Js <: MaybeHasJsWhereClause] {
  def meta: M with MongoMetaRecord[M]
  def master: MongoMetaRecord[M]
  def where[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def and[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def scan[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def iscan[F](clause: M => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def jsWhere[F](js: String)(implicit ev: Js =:= HasNoJsWhereClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, HasJsWhereClause]

  def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def jsWhereOpt[F](jsOpt: Option[String])(implicit ev: Js =:= HasNoJsWhereClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, HasJsWhereClause]

  def raw(f: BasicDBObjectBuilder => Unit): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
    
  def or(subqueries: (M with MongoMetaRecord[M] => AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _, _])*)(implicit ev: Or =:= HasNoOrClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause, Js]

  def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]
  def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]
  def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]
  def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered): AbstractQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]

  def limit(n: Int)(implicit ev: Lim =:= Unlimited): AbstractQuery[M, R, Ord, Sel, Limited, Sk, Or, Js]
  def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): AbstractQuery[M, R, Ord, Sel, Limited, Sk, Or, Js]
  def skip(n: Int)(implicit ev: Sk =:= Unskipped): AbstractQuery[M, R, Ord, Sel, Lim, Skipped, Or, Js]
  def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): AbstractQuery[M, R, Ord, Sel, Lim, Skipped, Or, Js]

  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long
  def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long
  def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean

  def foreach(f: R => Unit): Unit
  def fetch(): List[R]
  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R]
  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T]
  def get()(implicit ev: Lim =:= Unlimited): Option[R]
  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): BasePaginatedQuery[M, R]

  def noop()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): AbstractModifyQuery[M]

  def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit
  def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit
  def findAndDeleteOne(): Option[R]

  def signature(): String
  def explain(): String

  def maxScan(max: Int): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
  def comment(c: String): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]

  def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected): AbstractQuery[M, F1, Ord, Selected, Lim, Sk, Or, Js]
  def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or, Js]
  def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or, Js]
  def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or, Js]
  def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M])(implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or, Js]
  def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M])(implicit ev: Sel =:= Unselected): AbstractQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or, Js]

  def selectCase[F1, CC](f: M => SelectField[F1, M], create: F1 => CC)(implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  def selectCase[F1, F2, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], create: (F1, F2) => CC)(implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], create: (F1, F2, F3) => CC)(implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], create: (F1, F2, F3, F4) => CC)(implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], create: (F1, F2, F3, F4, F5) => CC)(implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], create: (F1, F2, F3, F4, F5, F6) => CC)(implicit ev: Sel =:= Unselected): AbstractQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]

  def hint(h: MongoIndex[M]): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
}

case class BaseQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped, Or <: MaybeHasOrClause, Js <: MaybeHasJsWhereClause](
    override val meta: M with MongoMetaRecord[M],
    lim: Option[Int],
    sk: Option[Int],
    maxScan: Option[Int],
    comment: Option[String],
    hint: Option[ListMap[String, Any]],
    condition: AndCondition,
    order: Option[MongoOrder],
    select: Option[MongoSelect[R, M]],
    js: Option[String]) extends AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js] {

  // The meta field on the MongoMetaRecord (as an instance of MongoRecord)
  // points to the master MongoMetaRecord. This is here in case you have a
  // second MongoMetaRecord pointing to the slave.
  override lazy val master = meta.meta

  private def addClause[F](clause: M => QueryClause[F], expectedIndexBehavior: IndexBehavior.Value): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js] = {
    clause(meta) match {
      case cl: EmptyQueryClause[_] => new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, Or, Js]
      case cl => {
        val newClause = cl.withExpectedIndexBehavior(expectedIndexBehavior)
        this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
      }
    }
  }

  override def where[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.Index)
  override def and[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.Index)
  override def iscan[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.IndexScan)
  override def scan[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.DocumentScan)
  override def jsWhere[F](js: String)(implicit ev: Js =:= HasNoJsWhereClause): BaseQuery[M, R, Ord, Sel, Lim, Sk, Or, HasJsWhereClause] =
    this.copy(js = Some(js))

  private def addClauseOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F], expectedIndexBehavior: IndexBehavior.Value) = {
    opt match {
      case Some(v) => addClause(clause(_, v), expectedIndexBehavior)
      case None => this
    }
  }

  override def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = addClauseOpt(opt)(clause, expectedIndexBehavior = IndexBehavior.Index)
  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = addClauseOpt(opt)(clause, expectedIndexBehavior = IndexBehavior.Index)
  override def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = addClauseOpt(opt)(clause, expectedIndexBehavior = IndexBehavior.IndexScan)
  override def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = addClauseOpt(opt)(clause, expectedIndexBehavior = IndexBehavior.DocumentScan)
  override def jsWhereOpt[F](jsOpt: Option[String])(implicit ev: Js =:= HasNoJsWhereClause): BaseQuery[M, R, Ord, Sel, Lim, Sk, Or, HasJsWhereClause] =
    this.copy(js = jsOpt)

  override def raw(f: BasicDBObjectBuilder => Unit) = {
    val newClause = new RawQueryClause(f)
    this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
  }
    
  override def or(subqueries: (M with MongoMetaRecord[M] => AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _, _])*)(implicit ev: Or =:= HasNoOrClause): AbstractQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause, Js] = {
    val orCondition = QueryHelpers.orConditionFromQueries(subqueries.toList.map(q => q(meta)))
    this.copy(condition = condition.copy(orCondition = Some(orCondition)))
  }

  override def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, true)))))
  override def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, false)))))
  override def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, true) :: order.get.terms)))
  override def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered): BaseQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, false) :: order.get.terms)))

  override def limit(n: Int)(implicit ev: Lim =:= Unlimited): BaseQuery[M, R, Ord, Sel, Limited, Sk, Or, Js] =
    this.copy(lim = Some(n))
  override def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): BaseQuery[M, R, Ord, Sel, Limited, Sk, Or, Js] =
    this.copy(lim = n)
  override def skip(n: Int)(implicit ev: Sk =:= Unskipped): BaseQuery[M, R, Ord, Sel, Lim, Skipped, Or, Js] =
    this.copy(sk = Some(n))
  override def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): BaseQuery[M, R, Ord, Sel, Lim, Skipped, Or, Js] =
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
            // Subfield select
            Box !! field.name.split('.').toList.foldLeft(dbo: Object){ case (obj, f) => obj.asInstanceOf[DBObject].get(f) }
          }
        }
      }
      setInstanceFieldFromDbo(inst.fieldByName("_id").open_!)
      transformer(fields.map(fld => fld(setInstanceFieldFromDbo(fld.field))))
    case None => meta.fromDBObject(dbo).asInstanceOf[R]
  }

  private def drainBuffer[A, B](from: ListBuffer[A], to: ListBuffer[B], f: List[A] => List[B], size: Int): Unit = {
    if (from.size >= size) {
      to ++= f(from.toList)
      from.clear
    }
  }

  override def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("count", this)(meta.count(_))
  override def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
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

  override def noop()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped) = BaseModifyQuery(this, MongoModify(Nil))

  // Always do modifications against master (not meta, which could point to slave)
  override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit =
    QueryExecutor.condition("remove", this)(master.bulkDelete_!!(_))
  override def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit =
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

  override def maxScan(max: Int): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js] = this.copy(maxScan = Some(max))
  override def comment(c: String): AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js] = this.copy(comment = Some(c))
  override def hint(index: MongoIndex[M]) = this.copy(hint = Some(index.asListMap))

  override def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, F1, Ord, Selected, Lim, Sk, Or, Js] = {
    selectCase(f, (f: F1) => f)
  }

  override def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or, Js] = {
    selectCase(f1, f2, (f1: F1, f2: F2) => (f1, f2))
  }

  override def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or, Js] = {
    selectCase(f1, f2, f3, (f1: F1, f2: F2, f3: F3) => (f1, f2, f3))
  }

  override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or, Js] = {
    selectCase(f1, f2, f3, f4, (f1: F1, f2: F2, f3: F3, f4: F4) => (f1, f2, f3, f4))
  }

  override def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or, Js] = {
    selectCase(f1, f2, f3, f4, f5, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5) => (f1, f2, f3, f4, f5))
  }

  override def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or, Js] = {
    selectCase(f1, f2, f3, f4, f5, f6, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6) => (f1, f2, f3, f4, f5, f6))
  }

  def selectCase[F1, CC](f: M => SelectField[F1, M], create: F1 => CC)(implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js] = {
    val inst = meta.createRecord
    val fields = List(f(inst))
    val transformer = (xs: List[_]) => create(xs.head.asInstanceOf[F1])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], create: (F1, F2) => CC)(implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], create: (F1, F2, F3) => CC)(implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], create: (F1, F2, F3, F4) => CC)(implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], create: (F1, F2, F3, F4, F5) => CC)(implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], create: (F1, F2, F3, F4, F5, F6) => CC)(implicit ev: Sel =:= Unselected): BaseQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }
}

class BaseEmptyQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped, Or <: MaybeHasOrClause, Js <: MaybeHasJsWhereClause] extends AbstractQuery[M, R, Ord, Sel, Lim, Sk, Or, Js] {
  override lazy val meta = throw new Exception("tried to read meta field of an EmptyQuery")
  override lazy val master = throw new Exception("tried to read master field of an EmptyQuery")
  override def where[F](clause: M => QueryClause[F]) = this
  override def and[F](clause: M => QueryClause[F]) = this
  override def iscan[F](clause: M => QueryClause[F]) = this
  override def scan[F](clause: M => QueryClause[F]) = this
  override def jsWhere[F](js: String)(implicit ev: Js =:= HasNoJsWhereClause) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, Or, HasJsWhereClause]

  override def whereOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this
  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this
  override def iscanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this
  override def scanOpt[V, F](opt: Option[V])(clause: (M, V) => QueryClause[F]) = this
  override def jsWhereOpt[F](jsOpt: Option[String])(implicit ev: Js =:= HasNoJsWhereClause) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, Or, HasJsWhereClause]

  override def raw(f: BasicDBObjectBuilder => Unit) = this    
    
  override def or(subqueries: (M with MongoMetaRecord[M] => AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _, _])*)(implicit ev: Or =:= HasNoOrClause) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause, Js]

  override def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]
  override def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]
  override def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]
  override def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk, Or, Js]

  override def limit(n: Int)(implicit ev: Lim =:= Unlimited) = new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk, Or, Js]
  override def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited) = new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk, Or, Js]
  override def skip(n: Int)(implicit ev: Sk =:= Unskipped) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped, Or, Js]
  override def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped, Or, Js]

  override def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0
  override def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0
  override def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean = false

  override def foreach(f: R => Unit): Unit = ()
  override def fetch(): List[R] = Nil
  override def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] = Nil
  override def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = Nil
  override def get()(implicit ev: Lim =:= Unlimited): Option[R] = None
  override def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    val emptyQuery = new BaseEmptyQuery[M, R, Ord, Sel, Unlimited, Unskipped, Or, Js]
    new BasePaginatedQuery(emptyQuery, countPerPage)
  }

  override def noop()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped) = new EmptyModifyQuery[M]

  override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit = ()
  override def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit = ()
  override def findAndDeleteOne(): Option[R] = None

  override def toString = "empty query"
  override def signature = "empty query"
  override def explain = "{}"

  override def maxScan(max: Int) = this
  override def comment(c: String) = this
  override def hint(index: MongoIndex[M]) = this

  override def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, F1, Ord, Selected, Lim, Sk, Or, Js]
  override def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or, Js]
  override def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or, Js]
  override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or, Js]
  override def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or, Js]
  override def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or, Js]

  override def selectCase[F1, CC](f: M => SelectField[F1, M], create: F1 => CC)(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  override def selectCase[F1, F2, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], create: (F1, F2) => CC)(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  override def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], create: (F1, F2, F3) => CC)(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  override def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], create: (F1, F2, F3, F4) => CC)(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  override def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], create: (F1, F2, F3, F4, F5) => CC)(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
  override def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], create: (F1, F2, F3, F4, F5, F6) => CC)(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, CC, Ord, Selected, Lim, Sk, Or, Js]
}

/////////////////////////////////////////////////////////
/// Modify Queries
/////////////////////////////////////////////////////////

trait AbstractModifyQuery[M <: MongoRecord[M]] {
  def modify[F](clause: M => ModifyClause[F]): AbstractModifyQuery[M]
  def and[F](clause: M => ModifyClause[F]): AbstractModifyQuery[M]

  def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractModifyQuery[M]
  def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractModifyQuery[M]

  def updateMulti(): Unit
  def updateOne(): Unit
  def upsertOne(): Unit
}

case class BaseModifyQuery[M <: MongoRecord[M]](query: BaseQuery[M, _, _ <: MaybeOrdered, _ <: MaybeSelected, _ <: MaybeLimited, _ <: MaybeSkipped, _ <: MaybeHasOrClause, _ <: MaybeHasJsWhereClause],
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

  override def modifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = addClauseOpt(opt)(clause)
  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = addClauseOpt(opt)(clause)

  // Always do modifications against master (not query.meta, which could point to slave)
  override def updateMulti(): Unit = QueryExecutor.modify("updateMulti", this)(query.master.updateMulti(_, _))
  override def updateOne(): Unit = QueryExecutor.modify("updateOne", this)(query.master.update(_, _))
  override def upsertOne(): Unit = QueryExecutor.modify("upsertOne", this)(query.master.upsert(_, _))

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

  override def toString = "empty modify query"
}


/////////////////////////////////////////////////////////
/// FindAndModify Queries
/////////////////////////////////////////////////////////

trait AbstractFindAndModifyQuery[M <: MongoRecord[M], R] {
  def findAndModify[F](clause: M => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]
  def and[F](clause: M => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

  def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]
  def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]): AbstractFindAndModifyQuery[M, R]

  def updateOne(returnNew: Boolean = false): Option[R]
  def upsertOne(returnNew: Boolean = false): Option[R]
}

case class BaseFindAndModifyQuery[M <: MongoRecord[M], R](query: BaseQuery[M, R, _ <: MaybeOrdered, _ <: MaybeSelected, _ <: MaybeLimited, _ <: MaybeSkipped, _ <: MaybeHasOrClause, _ <: MaybeHasJsWhereClause],
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

  override def findAndModifyOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = addClauseOpt(opt)(clause)
  override def andOpt[V, F](opt: Option[V])(clause: (M, V) => ModifyClause[F]) = addClauseOpt(opt)(clause)

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

class BasePaginatedQuery[M <: MongoRecord[M], R](q: AbstractQuery[M, R, _, _, Unlimited, Unskipped, _, _], val countPerPage: Int, val pageNum: Int = 1) {
  def copy() = new BasePaginatedQuery(q, countPerPage, pageNum)
  def setPage(p: Int) = if (p == pageNum) this else new BasePaginatedQuery(q, countPerPage, p)
  def setCountPerPage(c: Int) = if (c == countPerPage) this else new BasePaginatedQuery(q, c, pageNum)
  lazy val countAll: Long = q.count
  def fetch(): List[R] = q.skip(countPerPage * (pageNum - 1)).limit(countPerPage).fetch()
  def numPages = math.ceil(countAll.toDouble / countPerPage.toDouble).toInt max 1
}
