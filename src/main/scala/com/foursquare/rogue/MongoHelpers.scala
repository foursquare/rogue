// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{BasicDBObjectBuilder, DBObject, DBCursor}
import net.liftweb.mongodb._
import net.liftweb.mongodb.record._
import scala.collection.immutable.ListMap

object MongoHelpers {
  sealed abstract class MongoCondition
  case class AndCondition(clauses: List[QueryClause[_]]) extends MongoCondition
  case class OrCondition(conditions: List[MongoCondition]) extends MongoCondition

  sealed case class MongoOrder(terms: List[(String, Boolean)])
  sealed case class MongoModify(clauses: List[ModifyClause[_]])
  sealed case class MongoSelect[R, M <: MongoRecord[M]](fields: List[SelectField[_, M]], transformer: List[_] => R)

  object MongoBuilder {
    def buildCondition(q: MongoCondition, signature: Boolean = false): DBObject = q match {
      case AndCondition(clauses) =>
        val builder = BasicDBObjectBuilder.start
        (clauses.groupBy(_.fieldName)
                .toList
                .sortBy { case (fieldName, _) => -clauses.indexWhere(_.fieldName == fieldName) }
                .foreach { case (name, cs) =>
          // Equality clauses look like { a : 3 }
          // but all other clauses look like { a : { $op : 3 }}
          // and can be chained like { a : { $gt : 2, $lt: 6 }}.
          // So if there is any equality clause, apply it (only) to the builder;
          // otherwise, chain the clauses.
          cs.filter(_.isInstanceOf[EqClause[_]]).headOption.map(_.extend(builder, signature)).getOrElse {
            builder.push(name)
            cs.foreach(_.extend(builder, signature))
            builder.pop
          }
        })
        builder.get

      case OrCondition(conditions) =>
        // Room for optimization here by manipulating the AST, e.g.,
        // { $or : [ { a : 1 }, { a : 2 } ] }  ==>  { a : { $in : [ 1, 2 ] }}
        BasicDBObjectBuilder.start("$or", QueryHelpers.list(conditions.map(buildCondition(_, signature = false)))).get
    }

    def buildOrder(o: MongoOrder): DBObject = {
      val builder = BasicDBObjectBuilder.start
      o.terms.reverse.foreach { case (field, ascending) => builder.add(field, if (ascending) 1 else -1) }
      builder.get
    }

    def buildModify(m: MongoModify): DBObject = {
      val builder = BasicDBObjectBuilder.start
      m.clauses.groupBy(_.operator).foreach{ case (op, cs) => {
        builder.push(op.toString)
        cs.foreach(_.extend(builder))
        builder.pop
      }}
      builder.get
    }

    def buildSelect[R, M <: MongoRecord[M]](s: MongoSelect[R, M]): DBObject = {
      buildSelectFromNames(s.fields.view.map(_.field.name))
    }

    def buildSelectFromNames(names: Iterable[String]): DBObject = {
      val builder = BasicDBObjectBuilder.start
      names.foreach(n => builder.add(n, 1))
      builder.get
    }

    def buildHint[R, M <: MongoRecord[M]](h: ListMap[String, Any]): DBObject = {
      val builder = BasicDBObjectBuilder.start
      h.foreach{ case (field, attr) => {
        builder.add(field, attr)
      }}
      builder.get
    }

    def buildQueryCommandString[M <: MongoRecord[M], R](command: QueryCommand[_, M, R]): String = {
      val query = command.query
      val sb = new StringBuilder("db.%s.%s".format(command.query.meta.collectionName, command.functionPrefix))
      sb.append(buildCondition(query.condition, signature = false).toString)
      query.select.foreach(s => sb.append(", " + buildSelect(s).toString))
      sb.append(")")
      query.order.foreach(o => sb.append(".sort(%s)" format buildOrder(o).toString))
      query.lim.foreach(l => sb.append(".limit(%d)" format l))
      query.sk.foreach(s => sb.append(".skip(%d)" format s))
      query.maxScan.foreach(m => sb.append("._addSpecial(\"$maxScan\", %d)" format m))
      query.comment.foreach(c => sb.append("._addSpecial(\"$comment\", \"%s\")" format c))
      query.hint.foreach(h => sb.append(".hint(%s)" format buildHint(h).toString))
      sb.append(command.functionSuffix)
      sb.toString
    }

    def buildQueryCommandSignature[M <: MongoRecord[M], R](command: QueryCommand[_, M, R]): String = {
      val query = command.query
      val sb = new StringBuilder("db.%s.%s".format(command.query.meta.collectionName, command.functionPrefix))
      sb.append(buildCondition(query.condition, signature = true).toString)
      sb.append(")")
      query.order.foreach(o => sb.append(".sort(%s)" format buildOrder(o).toString))
      sb.append(command.functionSuffix)
      sb.toString
    }

    def buildModifyQueryCommandString[M <: MongoRecord[M]](command: ModifyQueryCommand[_, M]): String =
      doBuildModifyQueryCommandString(command, false)

    def buildModifyQueryCommandSignature[M <: MongoRecord[M]](command: ModifyQueryCommand[_, M]): String =
      doBuildModifyQueryCommandString(command, true)

    private def doBuildModifyQueryCommandString[M <: MongoRecord[M]](command: ModifyQueryCommand[_, M], signature: Boolean): String = {
      val modify = command.modify
      "db.%s.update(%s, %s, %s, %s)".format(
        modify.query.meta.collectionName,
        buildCondition(modify.query.condition, signature).toString,
        buildModify(modify.mod),
        command.upsert,
        command.multi
      )
    }
  }

  object QueryExecutor {

    import QueryHelpers._
    import MongoHelpers.MongoBuilder._

    private[rogue] def runCommand[T, M <: MongoRecord[M]](command: QueryCommand[_, M, _])(f: => T): T = {
      val start = System.currentTimeMillis
      try {
        f
      } catch {
        case e: Exception =>
          throw new RogueException("Mongo query on %s [%s] failed after %d ms".format(command.query.meta.mongoIdentifier,
            command.toString, System.currentTimeMillis - start), e)
      } finally {
        logger.log(command, System.currentTimeMillis - start)
      }
    }

    private[rogue] def runModifyCommand[T, M <: MongoRecord[M]](command: ModifyQueryCommand[_, M])(f: => T): T = {
      val start = System.currentTimeMillis
      try {
        f
      } catch {
        case e: Exception =>
          throw new RogueException("Mongo modify query on %s [%s] failed after %d ms".format(command.modify.query.meta.mongoIdentifier,
            command.toString, System.currentTimeMillis - start), e)
      } finally {
        logger.log(command, System.currentTimeMillis - start)
      }
    }

    def condition[M <: MongoRecord[M], T](command: ConditionQueryCommand[_, M, _])(f: DBObject => T): T = {
      val query = command.query
      validator.validateQuery(query)
      val cnd = buildCondition(query.condition)
      runCommand(command) {
        f(cnd)
      }
    }

    def modify[M <: MongoRecord[M], T](command: ModifyQueryCommand[T, M])
                                      (f: (DBObject, DBObject) => T): Unit = {
      val modify = command.modify
      validator.validateModify(modify)
      if (!modify.mod.clauses.isEmpty) {
        val q = buildCondition(modify.query.condition)
        val m = buildModify(modify.mod)

        runModifyCommand(command) {
          f(q, m)
        }
      }
    }

    def query[M <: MongoRecord[M]](command: FindQueryCommand[M, _])(f: DBObject => Unit): Unit = {
      val batchSize = command.batchSize
      doQuery(command) { cursor =>
        batchSize.foreach(cursor batchSize _)
        while (cursor.hasNext)
          f(cursor.next)
      }
    }

    def explain[M <: MongoRecord[M]](command: FindQueryCommand[M, _]): String = {
      var explanation = ""
      doQuery(command) { cursor =>
        explanation += cursor.explain.toString
      }
      explanation
    }

    private[rogue] def doQuery[M <: MongoRecord[M]](command: FindQueryCommand[M, _])(f: DBCursor  => Unit): Unit = {
      val query = command.query
      validator.validateQuery(query)
      val cnd = buildCondition(query.condition)
      val ord = query.order.map(buildOrder)
      val sel = query.select.map(buildSelect) getOrElse buildSelectFromNames(query.meta.metaFields.view.map(_.name))
      val hnt = query.hint.map(buildHint)

      runCommand(command) {
        MongoDB.useCollection(query.meta.mongoIdentifier, query.meta.collectionName) { coll =>
          try {
            val cursor = coll.find(cnd, sel).limit(query.lim getOrElse 0).skip(query.sk getOrElse 0)
            ord.foreach(cursor sort _)
            query.maxScan.foreach(cursor addSpecial("$maxScan", _))
            query.comment.foreach(cursor addSpecial("$comment", _))
            hnt.foreach(cursor hint _)
            f(cursor)
          } catch {
            case e: Exception =>
              throw new RogueException("Mongo query on %s [%s] failed".format(coll.getDB().getMongo().toString(),
                command.toString), e)
          }
        }
      }
    }
  }
}
