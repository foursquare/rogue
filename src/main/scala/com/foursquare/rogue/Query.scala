// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.rogue.MongoHelpers._
import com.mongodb.DBObject
import net.liftweb.mongodb.record._

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
    val select: Option[MongoSelect[R]]) {

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

  private def extract(fn: R => Unit): DBObject => Unit = select match {
    case Some(MongoSelect(fields, transformer)) => (dbo) => {
      val inst = meta.createRecord
      fn(transformer(fields.map(f => f(inst.fieldByName(f.field.name).open_!.setFromAny(dbo.get(f.field.name))))))
    }
    case None => 
      dbo => fn(meta.fromDBObject(dbo).asInstanceOf[R])
  }

  private def collect[T](f: (T => Unit) => Unit): List[T] = {
    val buf = new scala.collection.mutable.ListBuffer[T]
    f{ t: T => buf += t }
    buf.toList
  }

  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("count", this)(meta.count(_))
  def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    QueryExecutor.condition("countDistinct", this)(meta.countDistinct(field(meta).field.name, _))
  def foreach(f: R => Unit): Unit =
    QueryExecutor.query("find", this)(extract(f))
  def fetch(): List[R] =
    collect[R](f => QueryExecutor.query("find", this)(extract(f)))
  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] =
    this.limit(limit).fetch()
  def get()(implicit ev: Lim =:= Unlimited): Option[R] =
    fetch(1).headOption
  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    val q = new BaseQuery[M, R, Ord, Sel, Unlimited, Unskipped](meta, lim, sk, condition, order, select)
    new BasePaginatedQuery(q, countPerPage)
  }
  def modify[F](clause: M => ModifyClause[F])(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped) =
    new ModifyQuery(this, MongoModify(List(clause(meta))))

  // Always do modifications against master (not meta, which could point to slave)
  def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit =
    QueryExecutor.condition("bulkDelete", this)(master.bulkDelete_!!(_))
  override def toString: String =
    MongoBuilder.buildString(this, None)

  def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, F1, Ord, Selected, Lim, Sk] = {
    val fields = List(f(meta))
    val transformer = (xs: List[_]) => xs.head.asInstanceOf[F1]
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }
  
  def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2), Ord, Selected, Lim, Sk] = {
    val fields = List(f1(meta), f2(meta))
    val transformer = (xs: List[_]) => (xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }

  def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk] = {
    val fields = List(f1(meta), f2(meta), f3(meta))
    val transformer = (xs: List[_]) => (xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3])
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }

  def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected): BaseQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk] = {
    val fields = List(f1(meta), f2(meta), f3(meta), f4(meta))
    val transformer = (xs: List[_]) => (xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4])
    new BaseQuery(meta, lim, sk, condition, order, Some(MongoSelect(fields, transformer)))
  }
  
  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): List[T] = {
    val paginatedQuery = paginate(batchSize)
    (1 to paginatedQuery.numPages).toList.flatMap(page => f(paginatedQuery.setPage(page).fetch))
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

  override def modify[F](clause: M => ModifyClause[F])(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped) = new NoopModifyQuery
  override def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit = ()
  override def toString = "empty query"

  override def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, F1, Ord, Selected, Lim, Sk]
  override def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2), Ord, Selected, Lim, Sk]
  override def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk]
  override def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected) = new BaseEmptyQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk]

  override def fetchBatch[T](batchSize: Int)(f: List[R] => List[T])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): List[T] = Nil
}

class ModifyQuery[M <: MongoRecord[M]](val query: BaseQuery[M, _, _, _, _, _],
                                       val modify: MongoModify) {
  def and[F](clause: M => ModifyClause[F]) = new ModifyQuery(query, MongoModify(clause(query.meta) :: modify.clauses))

  // Always do modifications against master (not query.meta, which could point to slave)
  def updateMulti(): Unit = QueryExecutor.modify("updateMulti", this)(query.master.updateMulti(_, _))
  def updateOne(): Unit = QueryExecutor.modify("updateOne", this)(query.master.update(_, _))
  def upsertOne(): Unit = QueryExecutor.modify("upsertOne", this)(query.master.upsert(_, _))

  override def toString =
    MongoBuilder.buildString(query, Some(modify))
}

class NoopModifyQuery[M <: MongoRecord[M]] extends ModifyQuery[M](
  null.asInstanceOf[BaseQuery[M, _, _, _, _, _]],
  null.asInstanceOf[MongoModify]) {
  override def and[F](clause: M => ModifyClause[F]) = this
  override def updateMulti(): Unit = ()
  override def updateOne(): Unit = ()
  override def upsertOne(): Unit = ()
}

class BasePaginatedQuery[M <: MongoRecord[M], R](q: BaseQuery[M, R, _, _, Unlimited, Unskipped], val countPerPage: Int, val pageNum: Int = 1) {
  def copy() = new BasePaginatedQuery(q, countPerPage, pageNum)
  def setPage(p: Int) = if (p == pageNum) this else new BasePaginatedQuery(q, countPerPage, p)
  def setCountPerPage(c: Int) = if (c == countPerPage) this else new BasePaginatedQuery(q, c, pageNum)
  lazy val countAll: Long = q.count
  def fetch(): List[R] = q.skip(countPerPage * (pageNum - 1)).limit(countPerPage).fetch()
  def numPages = math.ceil(countAll.toDouble / countPerPage.toDouble).toInt max 1
}
