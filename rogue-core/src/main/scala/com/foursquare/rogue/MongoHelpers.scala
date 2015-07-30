// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{BasicDBObjectBuilder, DBObject}
import java.util.regex.Pattern
import scala.collection.immutable.ListMap

object MongoHelpers extends Rogue {
  case class AndCondition(clauses: List[QueryClause[_]], orCondition: Option[OrCondition]) {
    def isEmpty: Boolean = clauses.isEmpty && orCondition.isEmpty
  }

  case class OrCondition(conditions: List[AndCondition])

  sealed case class MongoOrder(terms: List[(String, Boolean)])

  sealed case class MongoModify(clauses: List[ModifyClause])

  sealed case class MongoSelect[M, R](fields: List[SelectField[_, M]], transformer: List[Any] => R)

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
            val (negative, positive) = cs.partition(_.negated)
            positive.foreach(_.extend(builder, signature))
            if (negative.nonEmpty) {
              builder.push("$not")
              negative.foreach(_.extend(builder, signature))
              builder.pop
            }
            builder.pop
          }
        }
      }}

      // Raw clauses
      rawClauses.foreach(_.extend(builder, signature))

      // Optional $or clause (only one per "and" chain)
      cond.orCondition.foreach(or => {
        val subclauses = or.conditions
            .map(buildCondition(_, signature))
            .filterNot(_.keySet.isEmpty)
        builder.add("$or", QueryHelpers.list(subclauses))
      })
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

    def buildSelect[M, R](select: MongoSelect[M, R]): DBObject = {
      val builder = BasicDBObjectBuilder.start
      // If select.fields is empty, then a MongoSelect clause exists, but has an empty
      // list of fields. In this case (used for .exists()), we select just the
      // _id field.
      if (select.fields.isEmpty) {
        builder.add("_id", 1)
      } else {
        select.fields.foreach(f => {
          f.slc match {
            case None => builder.add(f.field.name, 1)
            case Some((s, None)) => builder.push(f.field.name).add("$slice", s).pop()
            case Some((s, Some(e))) => builder.push(f.field.name).add("$slice", QueryHelpers.makeJavaList(List(s, e))).pop()
          }
        })
      }
      builder.get
    }

    def buildHint(h: ListMap[String, Any]): DBObject = {
      val builder = BasicDBObjectBuilder.start
      h.foreach{ case (field, attr) => {
        builder.add(field, attr)
      }}
      builder.get
    }

    val OidPattern = Pattern.compile("""\{ "\$oid" : "([0-9a-f]{24})"\}""")
    def stringFromDBObject(dbo: DBObject): String = {
      // DBObject.toString renders ObjectIds like { $oid: "..."" }, but we want ObjectId("...")
      // because that's the format the Mongo REPL accepts.
      OidPattern.matcher(dbo.toString).replaceAll("""ObjectId("$1")""")
    }

    def buildQueryString[R, M](operation: String, collectionName: String, query: Query[M, R, _]): String = {
      val sb = new StringBuilder("db.%s.%s(".format(collectionName, operation))
      sb.append(stringFromDBObject(buildCondition(query.condition, signature = false)))
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

    def buildConditionString[R, M](operation: String, collectionName: String, query: Query[M, R, _]): String = {
      val sb = new StringBuilder("db.%s.%s(".format(collectionName, operation))
      sb.append(buildCondition(query.condition, signature = false).toString)
      sb.append(")")
      sb.toString
    }

    def buildModifyString[R, M](collectionName: String, modify: ModifyQuery[M, _],
                                upsert: Boolean = false, multi: Boolean = false): String = {
      "db.%s.update(%s, %s, %s, %s)".format(
        collectionName,
        stringFromDBObject(buildCondition(modify.query.condition, signature = false)),
        stringFromDBObject(buildModify(modify.mod)),
        upsert,
        multi
      )
    }

    def buildFindAndModifyString[R, M](collectionName: String, mod: FindAndModifyQuery[M, R], returnNew: Boolean, upsert: Boolean, remove: Boolean): String = {
      val query = mod.query
      val sb = new StringBuilder("db.%s.findAndModify({ query: %s".format(
          collectionName, stringFromDBObject(buildCondition(query.condition))))
      query.order.foreach(o => sb.append(", sort: " + buildOrder(o).toString))
      if (remove) sb.append(", remove: true")
      sb.append(", update: " + stringFromDBObject(buildModify(mod.mod)))
      sb.append(", new: " + returnNew)
      query.select.foreach(s => sb.append(", fields: " + buildSelect(s).toString))
      sb.append(", upsert: " + upsert)
      sb.append(" })")
      sb.toString
    }

    def buildSignature[R, M](collectionName: String, query: Query[M, R, _]): String = {
      val sb = new StringBuilder("db.%s.find(".format(collectionName))
      sb.append(buildCondition(query.condition, signature = true).toString)
      sb.append(")")
      query.order.foreach(o => sb.append(".sort(%s)" format buildOrder(o).toString))
      sb.toString
    }
  }
}
