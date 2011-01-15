// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{BasicDBObjectBuilder, DBObject}
import net.liftweb.mongodb._
import net.liftweb.mongodb.record._

object MongoHelpers {
  sealed abstract class MongoCondition
  case class AndCondition(clauses: List[QueryClause[_]]) extends MongoCondition
  case class OrCondition(conditions: List[MongoCondition]) extends MongoCondition

  sealed case class MongoOrder(terms: List[(String, Boolean)])
  sealed case class MongoModify(clauses: List[ModifyClause[_]])
  sealed case class MongoSelect[R](fields: List[SelectField[_, _]], transformer: List[_] => R)

  object MongoBuilder {
    def buildCondition(q: MongoCondition): DBObject = q match {
      case AndCondition(clauses) =>
        val builder = BasicDBObjectBuilder.start
        (clauses.groupBy(_.fieldName)
                .toList
                .sortBy { case (fieldName, _) => -clauses.findIndexOf(_.fieldName == fieldName) }
                .foreach { case (name, cs) =>
          // Equality clauses look like { a : 3 }
          // but all other clauses look like { a : { $op : 3 }}
          // and can be chained like { a : { $gt : 2, $lt: 6 }}.
          // So if there is any equality clause, apply it (only) to the builder;
          // otherwise, chain the clauses.
          cs.filter(_.isInstanceOf[EqClause[_]]).headOption.map(_.extend(builder)).getOrElse {
            builder.push(name)
            cs.foreach(_.extend(builder))
            builder.pop
          }
        })
        builder.get

      case OrCondition(conditions) => 
        // Room for optimization here by manipulating the AST, e.g.,
        // { $or : [ { a : 1 }, { a : 2 } ] }  ==>  { a : { $in : [ 1, 2 ] }}
        BasicDBObjectBuilder.start("$or", QueryHelpers.list(conditions.map(buildCondition))).get
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
      val builder = BasicDBObjectBuilder.start
      s.fields.foreach(f => builder.add(f.field.name, 1))
      builder.get
    }

    def buildString[R](query: BaseQuery[_, R, _, _, _, _],
                       modify: Option[MongoModify]): String = {
      val sb = new StringBuilder
      sb.append(buildCondition(query.condition).toString)
      query.order.foreach(o => sb.append(" order by " + buildOrder(o).toString))
      query.select.foreach(s => sb.append(" select " + buildSelect(s).toString))
      query.lim.foreach(l => sb.append(" limit " + l))
      query.sk.foreach(s => sb.append(" skip " + s))
      modify.foreach(m => sb.append(" modify with " + buildModify(m)))
      sb.toString
    }
  }

  object QueryExecutor {

    import QueryHelpers._
    import MongoHelpers.MongoBuilder._

    def condition[M <: MongoRecord[M], T](operation: String,
                                            query: BaseQuery[M, _, _, _, _, _])
                                           (f: DBObject => T): T = {
      val start = System.currentTimeMillis
      val collection = query.meta.collectionName
      val cnd = buildCondition(query.condition)
      try {
        f(cnd)
      } finally {
        logger.log("Mongo %s.%s (%s)" format (collection, operation, cnd), System.currentTimeMillis - start)
      }
    }

    def modify[M <: MongoRecord[M], T](operation: String,
                                         mod: ModifyQuery[M])
                                        (f: (DBObject, DBObject) => T): T = {
      val start = System.currentTimeMillis
      val collection = mod.query.meta.collectionName
      val q = buildCondition(mod.query.condition)
      val m = buildModify(mod.modify)
      try {
        f(q, m)
      } finally {
        logger.log("Mongo %s.%s (%s, %s)" format (collection, operation, q, m), System.currentTimeMillis - start)
      }
    }

    def query[M <: MongoRecord[M]](operation: String,
                                     query: BaseQuery[M, _, _, _, _, _])
                                    (f: DBObject => Unit): Unit = {
      val start = System.currentTimeMillis
      MongoDB.useCollection(query.meta.mongoIdentifier, query.meta.collectionName) { coll =>
        val collection = coll.getName
        val cnd = buildCondition(query.condition)
        val ord = query.order.map(buildOrder)
        val sel = query.select.map(buildSelect)
        lazy val empty = BasicDBObjectBuilder.start.get
        try {
          val cursor = coll.find(cnd, sel getOrElse empty).limit(query.lim getOrElse 0).skip(query.sk getOrElse 0)
          ord.foreach(cursor sort _)
          while (cursor.hasNext)
            f(cursor.next)
        } finally {
          logger.log( {
            val str = new StringBuilder("Mongo " + collection +"." + operation)
            str.append("("+cnd)
            sel.foreach(s => str.append(", "+s))
            str.append(")")
            ord.foreach(o => str.append(".sort("+o+")"))
            query.sk.foreach(sk => str.append(".skip("+sk+")"))
            query.lim.foreach(l => str.append(".limit("+l+")"))
            str.toString
          }, System.currentTimeMillis - start)
        }
      }
    }
  }
}
