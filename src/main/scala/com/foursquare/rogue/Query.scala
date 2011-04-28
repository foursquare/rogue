// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers._
import com.mongodb.DBObject
import net.liftweb.mongodb.record._
import scala.collection.mutable.ListBuffer

/////////////////////////////////////////////////////////////////////////////
// Phantom types
/////////////////////////////////////////////////////////////////////////////

abstract sealed class Ordered
abstract sealed class Unordered
abstract sealed class Selected
abstract sealed class Unselected
abstract sealed class Limited
abstract sealed class Unlimited
abstract sealed class Skipped
abstract sealed class Unskipped

/////////////////////////////////////////////////////////////////////////////
// Builders
/////////////////////////////////////////////////////////////////////////////

class BaseQuery[M <: MongoRecord[M], R, Ord, Sel, Lim, Sk](
    val meta: M with MongoMetaRecord[M],
    val lim: Option[Int],
    val sk: Option[Int],
    val condition: AndCondition,
    val order: Option[MongoOrder],
    val select: Option[MongoSelect[R, M]]) {

  // The meta field on the MongoMetaRecord (as an instance of MongoRecord)
  // points to the master MongoMetaRecord
  lazy val master = meta.meta

  def where[F](clause: M => QueryClause[F]) = {
    clause(meta) match {
      case cl: EmptyQueryClause[_] => new BaseEmptyQuery[M, R, Ord, Sel, Lim, Sk]
      case cl => new BaseQuery[M, R, Ord, Sel, Lim, Sk](meta, lim, sk, AndCondition(cl :: condition.clauses), order, select)
    }
  }
  def and[F](clause: M => QueryClause[F]) = where(clause)
  def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) =
    new BaseQuery[M, R, Ordered, Sel, Lim, Sk](meta, lim, sk, condition, Some(MongoOrder(List((field(meta).field.name, true)))), select)
  def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) =
    new BaseQuery[M, R, Ordered, Sel, Lim, Sk](meta, lim, sk, condition, Some(MongoOrder(List((field(meta).field.name, false)))), select)
  def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) =
    new BaseQuery[M, R, Ordered, Sel, Lim, Sk](meta, lim, sk, condition, Some(MongoOrder((field(meta).field.name, true) :: order.get.terms)), select)
  def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) =
    new BaseQuery[M, R, Ordered, Sel, Lim, Sk](meta, lim, sk, condition, Some(MongoOrder((field(meta).field.name, false) :: order.get.terms)), select)

  def limit(n: Int)(implicit ev: Lim =:= Unlimited) =
    new BaseQuery[M, R, Ord, Sel, Limited, Sk](meta, Some(n), sk, condition, order, select)
  def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited) =
    new BaseQuery[M, R, Ord, Sel, Limited, Sk](meta, n, sk, condition, order, select)
  def skip(n: Int)(implicit ev: Sk =:= Unskipped) =
    new BaseQuery[M, R, Ord, Sel, Lim, Skipped](meta, lim, Some(n), condition, order, select)

  private def parseDBObject(dbo: DBObject): R = select match {
    case Some(MongoSelect(fields, transformer)) =>
      val inst = fields.head.field.owner
      def setInstanceFieldFromDbo(fieldName: String) = inst.fieldByName(fieldName).open_!.setFromAny(dbo.get(fieldName))
      setInstanceFieldFromDbo("_id")
      transformer(fields.map(fld => fld(setInstanceFieldFromDbo(fld.field.name))))
    case None => meta.fromDBObject(dbo).asInstanceOf[R]
  }

  private def drainBuffer[A, B](from: ListBuffer[A], to: ListBuffer[B], f: List[A] => List[B], size: Int): Unit = {
    if (from.size >= size) {
      to ++= f(from.toList)
      from.clear
    }
  }

  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("count", this)(meta.count(_))
  def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("countDistinct", this)(meta.countDistinct(field(meta).field.name, _))
  def foreach(f: R => Unit): Unit =
    QueryExecutor.query("find", this, None)(dbo => f(parseDBObject(dbo)))
  def fetch(): List[R] = {
    val rv = new ListBuffer[R]
    QueryExecutor.query("find", this, None)(dbo => rv += parseDBObject(dbo))
    rv.toList
  }
  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] =
    this.limit(limit).fetch()
  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = {
    val rv = new ListBuffer[T]
    val buf = new ListBuffer[R]

    QueryExecutor.query("find", this, Some(batchSize)) { dbo =>
      buf += parseDBObject(dbo)
      drainBuffer(buf, rv, f, batchSize)
    }
    drainBuffer(buf, rv, f, 1)

    rv.toList
  }
  def get()(implicit ev: Lim =:= Unlimited): Option[R] =
    fetch(1).headOption
  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    val q = new BaseQuery[M, R, Ord, Sel, Unlimited, Unskipped](meta, lim, sk, condition, order, select)
    new BasePaginatedQuery(q, countPerPage)
  }

  // Always do modifications against master (not meta, which could point to slave)
  def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit =
    QueryExecutor.condition("bulkDelete", this)(master.bulkDelete_!!(_))
  override def toString: String =
    MongoBuilder.buildString(this, None)
  def signature(): String =
    MongoBuilder.buildSignature(this)

  def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, F1, Ord, Selected, Lim, Sk] = {
    val inst = meta.createRecord
    val fields = List(f(inst))
    val transformer = (xs: List[_]) => xs.head.asInstanceOf[F1]
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }
  
  def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2), Ord, Selected, Lim, Sk] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst))
    val transformer = (xs: List[_]) => (xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }

  def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst))
    val transformer = (xs: List[_]) => (xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3])
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }

  def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst))
    val transformer = (xs: List[_]) => (xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4])
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }
}

class BaseEmptyQuery[M <: MongoRecord[M], R, Ord, Sel, Lim, Sk] extends BaseQuery[M, R, Ord, Sel, Lim, Sk](null.asInstanceOf[M with MongoMetaRecord[M]], None, None, null.asInstanceOf[AndCondition], None, None) {
  override def where[F](clause: M => QueryClause[F]) = this
  override def and[F](clause: M => QueryClause[F]) = this
  override def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk]
  override def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk]
  override def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk]
  override def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered) = new BaseEmptyQuery[M, R, Ordered, Sel, Lim, Sk]
  override def limit(n: Int)(implicit ev: Lim =:= Unlimited) = new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk]
  override def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited) = new BaseEmptyQuery[M, R, Ord, Sel, Limited, Sk]
  override def skip(n: Int)(implicit ev: Sk =:= Unskipped) = new BaseEmptyQuery[M, R, Ord, Sel, Lim, Skipped]

  override def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0
  override def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long = 0
  override def foreach(f: R => Unit): Unit = ()
  override def fetch(): List[R] = Nil
  override def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] = Nil
  override def get()(implicit ev: Lim =:= Unlimited): Option[R] = None
  override def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    val emptyQuery = new BaseEmptyQuery[M, R, Ord, Sel, Unlimited, Unskipped]
    new BasePaginatedQuery(emptyQuery, countPerPage)
  }

  override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit = ()
  override def toString = "empty query"
  override def signature = "empty query"

  override def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, F1, Ord, Selected, Lim, Sk]
  override def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2), Ord, Selected, Lim, Sk]
  override def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk]
  override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk]

  override def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = Nil
}

class ModifyQuery[M <: MongoRecord[M]](val query: BaseQuery[M, _, _, _, _, _],
                                       val mod: MongoModify) {
  def modify[F](clause: M => ModifyClause[F]) = {
    query match {
      case q: BaseEmptyQuery[_, _, _, _, _, _] => new NoopModifyQuery[M]
      case _ => new ModifyQuery(query, MongoModify(clause(query.meta) :: mod.clauses))
    }
  }
  def and[F](clause: M => ModifyClause[F]) = modify(clause)
  def noop() = new ModifyQuery(query, MongoModify(Nil))

  // Always do modifications against master (not query.meta, which could point to slave)
  def updateMulti(): Unit = QueryExecutor.modify("updateMulti", this)(query.master.updateMulti(_, _))
  def updateOne(): Unit = QueryExecutor.modify("updateOne", this)(query.master.update(_, _))
  def upsertOne(): Unit = QueryExecutor.modify("upsertOne", this)(query.master.upsert(_, _))

  override def toString =
    MongoBuilder.buildString(query, Some(mod))
}

class NoopModifyQuery[M <: MongoRecord[M]] extends ModifyQuery[M](
  null.asInstanceOf[BaseQuery[M, _, _, _, _, _]],
  null.asInstanceOf[MongoModify]) {
  override def and[F](clause: M => ModifyClause[F]) = this
  override def updateMulti(): Unit = ()
  override def updateOne(): Unit = ()
  override def upsertOne(): Unit = ()
  override def toString = "empty modify query"
}

class BasePaginatedQuery[M <: MongoRecord[M], R](q: BaseQuery[M, R, _, _, Unlimited, Unskipped], val countPerPage: Int, val pageNum: Int = 1) {
  def copy() = new BasePaginatedQuery(q, countPerPage, pageNum)
  def setPage(p: Int) = if (p == pageNum) this else new BasePaginatedQuery(q, countPerPage, p)
  def setCountPerPage(c: Int) = if (c == countPerPage) this else new BasePaginatedQuery(q, c, pageNum)
  lazy val countAll: Long = q.count
  def fetch(): List[R] = q.skip(countPerPage * (pageNum - 1)).limit(countPerPage).fetch()
  def numPages = math.ceil(countAll.toDouble / countPerPage.toDouble).toInt max 1
}
