// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{BasicDBObjectBuilder, Bytes, DBObject, DBCursor, WriteConcern}
import net.liftweb.mongodb._
import net.liftweb.mongodb.record._
import scala.collection.immutable.ListMap

object MongoHelpers {
  case class AndCondition(clauses: List[QueryClause[_]], orCondition: Option[OrCondition])

  case class OrCondition(conditions: List[AndCondition])

  sealed case class MongoOrder(terms: List[(String, Boolean)])

  sealed case class MongoModify(clauses: List[ModifyClause[_]])

  sealed case class MongoSelect[R, M <: MongoRecord[M]](fields: List[SelectField[_, M]],
                                                        transformer: List[_] => R)

  private type PlainBaseQuery[M <: MongoRecord[M], R] = BaseQuery[M, R, _, _, _, _, _]

  object MongoBuilder {
    def buildCondition(cond: AndCondition, signature: Boolean = false): DBObject = {
      buildCondition(cond, BasicDBObjectBuilder.start, signature)
    }

    def buildCondition(cond: AndCondition,
                       builder: BasicDBObjectBuilder,
                       signature: Boolean): DBObject = {
      val (rawClauses, safeClauses) = cond.clauses.partition(_.isInstanceOf[RawQueryClause])

      // Normal clauses
      safeClauses.groupBy(_.fieldName).toList
          .sortBy{ case (fieldName, _) => -cond.clauses.indexWhere(_.fieldName == fieldName) }
          .foreach{ case (name, cs) => {
        // Equality clauses look like { a : 3 }
        // but all other clauses look like { a : { $op : 3 }}
        // and can be chained like { a : { $gt : 2, $lt: 6 }}.
        // So if there is any equality clause, apply it (only) to the builder;
        // otherwise, chain the clauses.
        cs.filter(_.isInstanceOf[EqClause[_, _]]).headOption match {
          case Some(eqClause) => eqClause.extend(builder, signature)
          case None => {
            builder.push(name)
            cs.foreach(_.extend(builder, signature))
            builder.pop
          }
        }
      }}

      // Raw clauses
      rawClauses.foreach(_.extend(builder, signature))

      // Optional $or clause (only one per "and" chain)
      cond.orCondition.foreach(or =>
        builder.add("$or", QueryHelpers.list(or.conditions.map(buildCondition(_, signature = false)))))
      builder.get
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

    def buildSelectFromNames(_names: Iterable[String]): DBObject = {
      // If _names is empty, then a MongoSelect clause exists, but has an empty
      // list of fields. In this case (used for .exists()), we select just the
      // _id field.
      val names = if (_names.isEmpty) List("_id") else _names
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

    def buildQueryString[R, M <: MongoRecord[M]](operation: String, query: PlainBaseQuery[M, R]): String = {
      val sb = new StringBuilder("db.%s.%s(".format(query.meta.collectionName, operation))
      sb.append(buildCondition(query.condition, signature = false).toString)
      query.select.foreach(s => sb.append(", " + buildSelect(s).toString))
      sb.append(")")
      query.order.foreach(o => sb.append(".sort(%s)" format buildOrder(o).toString))
      query.lim.foreach(l => sb.append(".limit(%d)" format l))
      query.sk.foreach(s => sb.append(".skip(%d)" format s))
      query.maxScan.foreach(m => sb.append("._addSpecial(\"$maxScan\", %d)" format m))
      query.comment.foreach(c => sb.append("._addSpecial(\"$comment\", \"%s\")" format c))
      query.hint.foreach(h => sb.append(".hint(%s)" format buildHint(h).toString))
      sb.toString
    }

    def buildConditionString[R, M <: MongoRecord[M]](operation: String, query: PlainBaseQuery[M, R]): String = {
      val sb = new StringBuilder("db.%s.%s(".format(query.meta.collectionName, operation))
      sb.append(buildCondition(query.condition, signature = false).toString)
      sb.append(")")
      sb.toString
    }

    def buildModifyString[R, M <: MongoRecord[M]](modify: BaseModifyQuery[M],
                                                  upsert: Boolean = false, multi: Boolean = false): String = {
      "db.%s.update(%s, %s, %s, %s)".format(
        modify.query.meta.collectionName,
        buildCondition(modify.query.condition, signature = false).toString,
        buildModify(modify.mod),
        upsert,
        multi
      )
    }

    def buildFindAndModifyString[R, M <: MongoRecord[M]](mod: BaseFindAndModifyQuery[M, R], returnNew: Boolean, upsert: Boolean, remove: Boolean): String = {
      val query = mod.query
      val sb = new StringBuilder("db.%s.findAndModify({ query: %s".format(
          query.meta.collectionName, buildCondition(query.condition)))
      query.order.foreach(o => sb.append(", sort: " + buildOrder(o).toString))
      if (remove) sb.append(", remove: true")
      sb.append(", update: " + buildModify(mod.mod).toString)
      sb.append(", new: " + returnNew)
      query.select.foreach(s => sb.append(", fields: " + buildSelect(s).toString))
      sb.append(", upsert: " + upsert)
      sb.append(" })")
      sb.toString
    }

    def buildSignature[R, M <: MongoRecord[M]](query: PlainBaseQuery[M, R]): String = {
      val sb = new StringBuilder("db.%s.find(".format(query.meta.collectionName))
      sb.append(buildCondition(query.condition, signature = true).toString)
      sb.append(")")
      query.order.foreach(o => sb.append(".sort(%s)" format buildOrder(o).toString))
      sb.toString
    }
  }

  object QueryExecutor {

    import QueryHelpers._
    import MongoHelpers.MongoBuilder._

    private[rogue] def runCommand[T](description: => String,
                                     query: PlainBaseQuery[_, _])(f: => T): T = {
      val start = System.currentTimeMillis
      try {
        f
      } catch {
        case e: Exception =>
          throw new RogueException("Mongo query on %s [%s] failed after %d ms".
                                   format(query.meta.mongoIdentifier,
            description, System.currentTimeMillis - start), e)
      } finally {
        logger.log(description, query.signature, System.currentTimeMillis - start)
      }
    }

    def condition[M <: MongoRecord[M], T](operation: String,
                                          query: PlainBaseQuery[M, _])
                                         (f: DBObject => T): T = {
      val queryClause = transformer.transformQuery(query)
      validator.validateQuery(queryClause)
      val cnd = buildCondition(queryClause.condition)
      val description = buildConditionString(operation, queryClause)

      runCommand(description, queryClause) {
        f(cnd)
      }
    }

    def modify[M <: MongoRecord[M], T](mod: BaseModifyQuery[M],
                                       upsert: Boolean,
                                       multi: Boolean,
                                       writeConcern: Option[WriteConcern] = None): Unit = {
      val modClause = transformer.transformModify(mod)
      validator.validateModify(modClause)
      if (!modClause.mod.clauses.isEmpty) {
        val q = buildCondition(modClause.query.condition)
        val m = buildModify(modClause.mod)
        lazy val description = buildModifyString(modClause, upsert = upsert, multi = multi)

        runCommand(description, modClause.query) {
          MongoDB.useSession(mod.query.master.mongoIdentifier) { db =>
            val coll = db.getCollection(mod.query.master.collectionName)
            writeConcern match {
              case Some(theWriteConcern) => coll.update(q, m, upsert, multi, theWriteConcern)
              case None => coll.update(q, m, upsert, multi)
            }
          }
        }
      }
    }

    def findAndModify[M <: MongoRecord[M], R](mod: BaseFindAndModifyQuery[M, R],
                                              returnNew: Boolean,
                                              upsert: Boolean,
                                              remove: Boolean)
                                             (f: DBObject => R): Option[R] = {
      val modClause = transformer.transformFindAndModify(mod)
      validator.validateFindAndModify(modClause)
      if (!modClause.mod.clauses.isEmpty || remove) {
        val query = modClause.query
        val cnd = buildCondition(query.condition)
        val ord = query.order.map(buildOrder)
        val sel = query.select.map(buildSelect) getOrElse
            buildSelectFromNames(query.meta.metaFields.view.map(_.name))
        val m = buildModify(modClause.mod)
        lazy val description = buildFindAndModifyString(modClause, returnNew, upsert, remove)

        runCommand(description, modClause.query) {
          MongoDB.useCollection(query.meta.mongoIdentifier, query.meta.collectionName) { coll =>
            val dbObj = coll.findAndModify(cnd, sel, ord.getOrElse(null), remove, m, returnNew, upsert)
            Option(dbObj).map(f)
          }
        }
      }
      else None
    }

    def query[M <: MongoRecord[M]](operation: String,
                                   query: PlainBaseQuery[M, _],
                                   batchSize: Option[Int])
                                  (f: DBObject => Unit): Unit = {
      doQuery(operation, query){cursor =>
        batchSize.foreach(cursor batchSize _)
        while (cursor.hasNext)
          f(cursor.next)
      }
    }

    def explain[M <: MongoRecord[M]](operation: String,
                                     query: PlainBaseQuery[M, _]): String = {
      var explanation = ""
      doQuery(operation, query){cursor =>
        explanation += cursor.explain.toString
      }
      explanation
    }

    private[rogue] def doQuery[M <: MongoRecord[M]](operation: String,
                                   query: PlainBaseQuery[M, _])
                                  (f: DBCursor  => Unit): Unit = {

      val queryClause = transformer.transformQuery(query)
      validator.validateQuery(queryClause)
      val cnd = buildCondition(queryClause.condition)
      val ord = queryClause.order.map(buildOrder)
      val sel = queryClause.select.map(buildSelect) getOrElse
                                    buildSelectFromNames(queryClause.meta.metaFields.view.map(_.name))
      val hnt = queryClause.hint.map(buildHint)

      lazy val description = buildQueryString(operation, queryClause)

      runCommand(description, queryClause){
        MongoDB.useCollection(queryClause.meta.mongoIdentifier, queryClause.meta.collectionName) {
          coll =>
          try {
            val cursor = coll.find(cnd, sel)
            queryClause.lim.foreach(cursor.limit _)
            queryClause.sk.foreach(cursor.skip _)
            ord.foreach(cursor.sort _)
            queryClause.slaveOk.foreach(so => {
              if (so) {
                // Use bitwise-or to add in slave-ok
                cursor.setOptions(cursor.getOptions | Bytes.QUERYOPTION_SLAVEOK)
              } else {
                // Remove slave-ok from options
                cursor.setOptions(cursor.getOptions & ~Bytes.QUERYOPTION_SLAVEOK)
              }
            })
            queryClause.maxScan.foreach(cursor addSpecial("$maxScan", _))
            queryClause.comment.foreach(cursor addSpecial("$comment", _))
            hnt.foreach(cursor hint _)
            f(cursor)
          } catch {
            case e: Exception =>
              throw new RogueException("Mongo query on %s [%s] failed".format(
                coll.getDB().getMongo().toString(), description), e)
          }
        }
      }
    }
  }
}
