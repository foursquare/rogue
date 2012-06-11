// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{BasicDBObjectBuilder, DBObject}
import scala.collection.immutable.ListMap

object MongoHelpers extends Rogue {
  case class AndCondition(clauses: List[QueryClause[_]], orCondition: Option[OrCondition])

  case class OrCondition(conditions: List[AndCondition])

  sealed case class MongoOrder(terms: List[(String, Boolean)])

  sealed case class MongoModify(clauses: List[ModifyClause[_]])

  sealed case class MongoSelect[R](fields: List[SelectField[_, _]], transformer: List[_] => R)

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

    def buildSelect[R](s: MongoSelect[R]): DBObject = {
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

    def buildHint(h: ListMap[String, Any]): DBObject = {
      val builder = BasicDBObjectBuilder.start
      h.foreach{ case (field, attr) => {
        builder.add(field, attr)
      }}
      builder.get
    }

    def buildQueryString[R, M](operation: String, collectionName: String, query: GenericBaseQuery[M, R]): String = {
      val sb = new StringBuilder("db.%s.%s(".format(collectionName, operation))
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

    def buildConditionString[R, M](operation: String, collectionName: String, query: GenericBaseQuery[M, R]): String = {
      val sb = new StringBuilder("db.%s.%s(".format(collectionName, operation))
      sb.append(buildCondition(query.condition, signature = false).toString)
      sb.append(")")
      sb.toString
    }

    def buildModifyString[R, M](collectionName: String, modify: BaseModifyQuery[M, _],
                                upsert: Boolean = false, multi: Boolean = false): String = {
      "db.%s.update(%s, %s, %s, %s)".format(
        collectionName,
        buildCondition(modify.query.condition, signature = false).toString,
        buildModify(modify.mod),
        upsert,
        multi
      )
    }

    def buildFindAndModifyString[R, M](collectionName: String, mod: BaseFindAndModifyQuery[M, R], returnNew: Boolean, upsert: Boolean, remove: Boolean): String = {
      val query = mod.query
      val sb = new StringBuilder("db.%s.findAndModify({ query: %s".format(
          collectionName, buildCondition(query.condition)))
      query.order.foreach(o => sb.append(", sort: " + buildOrder(o).toString))
      if (remove) sb.append(", remove: true")
      sb.append(", update: " + buildModify(mod.mod).toString)
      sb.append(", new: " + returnNew)
      query.select.foreach(s => sb.append(", fields: " + buildSelect(s).toString))
      sb.append(", upsert: " + upsert)
      sb.append(" })")
      sb.toString
    }

    def buildSignature[R, M](collectionName: String, query: GenericBaseQuery[M, R]): String = {
      val sb = new StringBuilder("db.%s.find(".format(collectionName))
      sb.append(buildCondition(query.condition, signature = true).toString)
      sb.append(")")
      query.order.foreach(o => sb.append(".sort(%s)" format buildOrder(o).toString))
      sb.toString
    }
  }
}
