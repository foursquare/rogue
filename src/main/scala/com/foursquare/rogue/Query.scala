// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import collection.immutable.List._
import com.foursquare.rogue.MongoHelpers._
import com.mongodb.{DBObject, WriteConcern}
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

/////////////////////////////////////////////////////////////////////////////
// Commands and Query Builders
/////////////////////////////////////////////////////////////////////////////

trait Command[T] {
  val id: String
  def execute(): T
  def signature: String
}

// A mongo command consists of a query (a set of criteria) and an operation (what to do for those criteria).
trait QueryCommand[T, M <: MongoRecord[M], R] extends Command[T] {
  def query: Rogue.PlainQuery[M, R]
  def functionPrefix: String
  def functionSuffix: String = ""

  val id = query.meta.mongoIdentifier.toString
  def signature: String = MongoBuilder.buildQueryCommandSignature(this)

  override def toString: String = MongoBuilder.buildQueryCommandString(this)
}

case class FindQueryCommand[M <: MongoRecord[M], R](query: Rogue.PlainQuery[M, R],
                                                    batchSize: Option[Int])
                                                   (f: DBObject => Unit) extends QueryCommand[Unit, M, R] {
  def execute(): Unit = QueryExecutor.query(this)(f)
  def functionPrefix: String = "find("
}

trait ConditionQueryCommand[T, M <: MongoRecord[M], R] extends QueryCommand[T, M, R] {
  val f: DBObject => T

  def execute(): T = QueryExecutor.condition(this)(f)
}

case class RemoveQueryCommand[M <: MongoRecord[M], R](query: Rogue.PlainQuery[M, R])
                                                     (val f: DBObject => Unit) extends ConditionQueryCommand[Unit, M, R] {
  def functionPrefix: String = "remove("
}

case class CountQueryCommand[M <: MongoRecord[M], R](query: Rogue.PlainQuery[M, R])
                                                    (val f: DBObject => Long) extends ConditionQueryCommand[Long, M, R] {
  def functionPrefix: String = "count("
}

case class CountDistinctQueryCommand[M <: MongoRecord[M], R](query: Rogue.PlainQuery[M, R],
                                                             fieldName: String)
                                                            (val f: DBObject => Long) extends ConditionQueryCommand[Long, M, R] {
  def functionPrefix: String = """distinct("%s", """.format(fieldName)
  override def functionSuffix = ".length"
}

// isEmpty is used to short-circuit the database call in the case that we already know that there can be no results.
// Note that if isEmpty is true then no operation on this object can yield a non-empty query (it propagates to returned
// objects via copy()).
case class BasicQuery[M <: MongoRecord[M], R, Ord <: MaybeOrdered, Sel <: MaybeSelected, Lim <: MaybeLimited, Sk <: MaybeSkipped, Or <: MaybeHasOrClause](
    meta: M with MongoMetaRecord[M],
    lim: Option[Int],
    sk: Option[Int],
    maxScan: Option[Int],
    comment: Option[String],
    hint: Option[ListMap[String, Any]],
    condition: AndCondition,
    order: Option[MongoOrder],
    select: Option[MongoSelect[M, R]],
    isEmpty: Boolean=false) {
  // The meta field on the MongoMetaRecord (as an instance of MongoRecord)
  // points to the master MongoMetaRecord. This is here in case you have a
  // second MongoMetaRecord pointing to the slave.
  lazy val master = meta.meta

  // A no-op if isEmpty == true.
  private def addClause[F](clause: M => QueryClause[F], expectedIndexBehavior: IndexBehavior.Value): BasicQuery[M, R, Ord, Sel, Lim, Sk, Or] = {
    if (isEmpty) {
      this
    } else {
      clause(meta) match {
        case cl: EmptyQueryClause[_] => this.copy(isEmpty=true)
        case cl => {
          val newClause = cl.withExpectedIndexBehavior(expectedIndexBehavior)
          this.copy(condition = condition.copy(clauses = newClause :: condition.clauses))
        }
      }
    }
  }

  def where[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.Index)
  def and[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.Index)
  def iscan[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.IndexScan)
  def scan[F](clause: M => QueryClause[F]) = addClause(clause, expectedIndexBehavior = IndexBehavior.DocumentScan)

  def or(subqueries: (M with MongoMetaRecord[M] => BasicQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, _])*)(implicit ev: Or =:= HasNoOrClause): BasicQuery[M, R, Ord, Sel, Lim, Sk, HasOrClause] = {
    val orCondition = QueryHelpers.orConditionFromQueries(subqueries.toList.map(q => q(meta)))
    this.copy(condition = condition.copy(orCondition = Some(orCondition)))
  }

  def orderAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered): BasicQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, true)))))
  def orderDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Unordered): BasicQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder(List((field(meta).field.name, false)))))
  def andAsc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered): BasicQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, true) :: order.get.terms)))
  def andDesc[V](field: M => QueryField[V, M])(implicit ev: Ord =:= Ordered): BasicQuery[M, R, Ordered, Sel, Lim, Sk, Or] =
    this.copy(order = Some(MongoOrder((field(meta).field.name, false) :: order.get.terms)))

  def limit(n: Int)(implicit ev: Lim =:= Unlimited): BasicQuery[M, R, Ord, Sel, Limited, Sk, Or] =
    this.copy(lim = Some(n))
  def limitOpt(n: Option[Int])(implicit ev: Lim =:= Unlimited): BasicQuery[M, R, Ord, Sel, Limited, Sk, Or] =
    this.copy(lim = n)
  def skip(n: Int)(implicit ev: Sk =:= Unskipped): BasicQuery[M, R, Ord, Sel, Lim, Skipped, Or] =
    this.copy(sk = Some(n))
  def skipOpt(n: Option[Int])(implicit ev: Sk =:= Unskipped): BasicQuery[M, R, Ord, Sel, Lim, Skipped, Or] =
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

  def count()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
    if (isEmpty) 0 else CountQueryCommand(this)(meta.count(_)).execute()

  def countDistinct[V](field: M => QueryField[V, M])(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Long =
  if (isEmpty) 0 else {
    val fieldName: String = field(meta).field.name
    CountDistinctQueryCommand(this, fieldName)(meta.countDistinct(fieldName, _)).execute()
  }

  def exists()(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped): Boolean =
    if (isEmpty) false else (this.copy(select = Some(MongoSelect[M, Null](Nil, _ => null))).limit(1).fetch().size > 0)

  def foreach(f: R => Unit): Unit =
    if (!isEmpty) FindQueryCommand(this, None)(dbo => f(parseDBObject(dbo))).execute()

  def fetch(): List[R] = if (isEmpty) Nil else {
    val rv = new ListBuffer[R]
    FindQueryCommand(this, None)(dbo => rv += parseDBObject(dbo)).execute()
    rv.toList
  }

  def fetch(limit: Int)(implicit ev: Lim =:= Unlimited): List[R] =
    if (isEmpty) Nil else this.limit(limit).fetch()

  def fetchBatch[T](batchSize: Int)(f: List[R] => List[T]): List[T] = if (isEmpty) Nil else {
    val rv = new ListBuffer[T]
    val buf = new ListBuffer[R]

    FindQueryCommand(this, Some(batchSize))({ dbo =>
      buf += parseDBObject(dbo)
      drainBuffer(buf, rv, f, batchSize)
    }).execute()
    drainBuffer(buf, rv, f, 1)

    rv.toList
  }

  def get()(implicit ev: Lim =:= Unlimited): Option[R] =
    if (isEmpty) None else fetch(1).headOption

  def paginate(countPerPage: Int)(implicit ev1: Lim =:= Unlimited, ev2: Sk =:= Unskipped) = {
    new PaginatedQuery(this.copy(), countPerPage)
  }

  def noop()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped) = ModifyQuery(this, MongoModify(Nil))

  // Always do modifications against master (not meta, which could point to slave)
  def bulkDelete_!!()(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit =
    if (!isEmpty) RemoveQueryCommand(this)(master.bulkDelete_!!(_)).execute()

  def blockingBulkDelete_!!(concern: WriteConcern)(implicit ev1: Sel =:= Unselected, ev2: Lim =:= Unlimited, ev3: Sk =:= Unskipped): Unit =
    if (!isEmpty) {
      RemoveQueryCommand(this)({ qry =>
        MongoDB.useCollection(master.mongoIdentifier, master.collectionName) { coll =>
          coll.remove(qry, concern)
        }
      }).execute()
    }

  // Since we don't know which command will be called on this query, we hard-code Find here.
  override def toString: String = FindQueryCommand(this, None)( _ => ()).toString
  def signature(): String = FindQueryCommand(this, None)( _ => ()).signature
  def explain(): String = if (isEmpty) "{}" else QueryExecutor.explain(FindQueryCommand(this, None)( _ => ()))

  def findAndDeleteOne(): Option[R] = if (isEmpty) None else {
    val mod = FindAndModifyQuery(this, MongoModify(Nil))
    FindAndModifyCommand(mod, returnNew=false, upsert=false, remove=true).execute()
  }

  def maxScan(max: Int): BasicQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(maxScan = Some(max))
  def comment(c: String): BasicQuery[M, R, Ord, Sel, Lim, Sk, Or] = this.copy(comment = Some(c))
  def hint(index: MongoIndex[M]) = this.copy(hint = Some(index.asListMap))

  def select[F1](f: M => SelectField[F1, M])(implicit ev: Sel =:= Unselected): BasicQuery[M, F1, Ord, Selected, Lim, Sk, Or] = {
    selectCase(f, (f: F1) => f)
  }

  def select[F1, F2](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M])(implicit ev: Sel =:= Unselected): BasicQuery[M, (F1, F2), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, (f1: F1, f2: F2) => (f1, f2))
  }

  def select[F1, F2, F3](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M])(implicit ev: Sel =:= Unselected): BasicQuery[M, (F1, F2, F3), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, (f1: F1, f2: F2, f3: F3) => (f1, f2, f3))
  }

  def select[F1, F2, F3, F4](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M])(implicit ev: Sel =:= Unselected): BasicQuery[M, (F1, F2, F3, F4), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, (f1: F1, f2: F2, f3: F3, f4: F4) => (f1, f2, f3, f4))
  }

  def select[F1, F2, F3, F4, F5](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M])(implicit ev: Sel =:= Unselected): BasicQuery[M, (F1, F2, F3, F4, F5), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5) => (f1, f2, f3, f4, f5))
  }

  def select[F1, F2, F3, F4, F5, F6](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M])(implicit ev: Sel =:= Unselected): BasicQuery[M, (F1, F2, F3, F4, F5, F6), Ord, Selected, Lim, Sk, Or] = {
    selectCase(f1, f2, f3, f4, f5, f6, (f1: F1, f2: F2, f3: F3, f4: F4, f5: F5, f6: F6) => (f1, f2, f3, f4, f5, f6))
  }

  def selectCase[F1, CC](f: M => SelectField[F1, M], create: F1 => CC)(implicit ev: Sel =:= Unselected): BasicQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f(inst))
    val transformer = (xs: List[_]) => create(xs.head.asInstanceOf[F1])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], create: (F1, F2) => CC)(implicit ev: Sel =:= Unselected): BasicQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], create: (F1, F2, F3) => CC)(implicit ev: Sel =:= Unselected): BasicQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], create: (F1, F2, F3, F4) => CC)(implicit ev: Sel =:= Unselected): BasicQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], create: (F1, F2, F3, F4, F5) => CC)(implicit ev: Sel =:= Unselected): BasicQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }

  def selectCase[F1, F2, F3, F4, F5, F6, CC](f1: M => SelectField[F1, M], f2: M => SelectField[F2, M], f3: M => SelectField[F3, M], f4: M => SelectField[F4, M], f5: M => SelectField[F5, M], f6: M => SelectField[F6, M], create: (F1, F2, F3, F4, F5, F6) => CC)(implicit ev: Sel =:= Unselected): BasicQuery[M, CC, Ord, Selected, Lim, Sk, Or] = {
    val inst = meta.createRecord
    val fields = List(f1(inst), f2(inst), f3(inst), f4(inst), f5(inst), f6(inst))
    val transformer = (xs: List[_]) => create(xs(0).asInstanceOf[F1], xs(1).asInstanceOf[F2], xs(2).asInstanceOf[F3], xs(3).asInstanceOf[F4], xs(4).asInstanceOf[F5], xs(5).asInstanceOf[F6])
    this.copy(select = Some(MongoSelect(fields, transformer)))
  }
}


/////////////////////////////////////////////////////////
/// Modify Queries
/////////////////////////////////////////////////////////

trait ModifyCommand[M <: MongoRecord[M]] extends Command[Unit] {
  def modify: ModifyQuery[M]
  val f: (DBObject, DBObject) => Unit
  val upsert: Boolean = false
  val multi: Boolean = false

  val id = modify.query.meta.mongoIdentifier.toString
  def execute(): Unit = QueryExecutor.modify(this)(f)
  def signature: String = MongoBuilder.buildModifyCommandSignature(this)

  override def toString(): String = MongoBuilder.buildModifyCommandString(this)
}

case class UpdateOneCommand[M <: MongoRecord[M]](modify: ModifyQuery[M]) extends ModifyCommand[M] {
  val f: (DBObject, DBObject) => Unit = modify.query.master.update(_, _)
}

case class UpsertOneCommand[M <: MongoRecord[M]](modify: ModifyQuery[M]) extends ModifyCommand[M] {
  override val upsert = true
  val f: (DBObject, DBObject) => Unit = modify.query.master.upsert(_, _)
}

case class UpdateMultiCommand[M <: MongoRecord[M]](modify: ModifyQuery[M]) extends ModifyCommand[M] {
  override val multi = true
  val f: (DBObject, DBObject) => Unit = modify.query.master.updateMulti(_, _)
}

case class ModifyQuery[M <: MongoRecord[M]](query: Rogue.PlainQuery[M, _], mod: MongoModify) {
  val isEmpty = query.isEmpty

  private def addClause[F](clause: M => ModifyClause[F]) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  def modify[F](clause: M => ModifyClause[F]) = addClause(clause)
  def and[F](clause: M => ModifyClause[F]) = addClause(clause)

  // Always do modifications against master (not query.meta, which could point to slave)
  def updateMulti(): Unit = if (!isEmpty) UpdateMultiCommand(this).execute()
  def updateOne(): Unit = if (!isEmpty) UpdateOneCommand(this).execute()
  def upsertOne(): Unit = if (!isEmpty) UpsertOneCommand(this).execute()

  // Since we don't know which operation will be called, we hard-code UpdateOne here.
  override def toString = UpdateOneCommand(this).toString
}


/////////////////////////////////////////////////////////
/// FindAndModify Queries
/////////////////////////////////////////////////////////

case class FindAndModifyCommand[M <: MongoRecord[M], R](modify: FindAndModifyQuery[M, R],
                                                        returnNew: Boolean,
                                                        upsert: Boolean,
                                                        remove: Boolean=false) extends Command[Option[R]] {
  val id = modify.query.meta.mongoIdentifier.toString
  def execute(): Option[R] = QueryExecutor.findAndModify(this)(modify.query.parseDBObject _)
  def signature: String = MongoBuilder.buildFindAndModifyCommandSignature(this)

  override def toString(): String = MongoBuilder.buildFindAndModifyCommandString(this)
}


case class FindAndModifyQuery[M <: MongoRecord[M], R](query: Rogue.PlainQuery[M, R], mod: MongoModify) {
  private val isEmpty = query.isEmpty

  private def addClause[F](clause: M => ModifyClause[F]) = {
    this.copy(mod = MongoModify(clause(query.meta) :: mod.clauses))
  }

  def findAndModify[F](clause: M => ModifyClause[F]) = addClause(clause)
  def and[F](clause: M => ModifyClause[F]) = addClause(clause)

  // Always do modifications against master (not query.meta, which could point to slave)
  def updateOne(returnNew: Boolean = false): Option[R] = if (isEmpty) None else
    FindAndModifyCommand(this, returnNew, upsert=false).execute()

  def upsertOne(returnNew: Boolean = false): Option[R] = if (isEmpty) None else
    FindAndModifyCommand(this, returnNew, upsert=true).execute()

  // We don't know what the settings will be, so we hard-code them here.
  override def toString = FindAndModifyCommand(this, false, false, false).toString
}


class PaginatedQuery[M <: MongoRecord[M], R](q: BasicQuery[M, R, _, _, Unlimited, Unskipped, _], val countPerPage: Int, val pageNum: Int = 1) {
  def copy() = new PaginatedQuery(q, countPerPage, pageNum)
  def setPage(p: Int) = if (p == pageNum) this else new PaginatedQuery(q, countPerPage, p)
  def setCountPerPage(c: Int) = if (c == countPerPage) this else new PaginatedQuery(q, c, pageNum)
  lazy val countAll: Long = q.count
  def fetch(): List[R] = q.skip(countPerPage * (pageNum - 1)).limit(countPerPage).fetch()
  def numPages = math.ceil(countAll.toDouble / countPerPage.toDouble).toInt max 1
}
