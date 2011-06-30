// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.mongodb.{BasicDBObjectBuilder, DBObject, DBCursor}
import net.liftweb.mongodb._
import net.liftweb.mongodb.record._

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
                .sortBy { case (fieldName, _) => -clauses.findIndexOf(_.fieldName == fieldName) }
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

    def buildString[R, M <: MongoRecord[M]](query: BaseQuery[M, R, _, _, _, _],
                       modify: Option[MongoModify]): String = {
      val sb = new StringBuilder
      sb.append(buildCondition(query.condition, signature = false).toString)
      query.order.foreach(o => sb.append(" order by " + buildOrder(o).toString))
      query.select.foreach(s => sb.append(" select " + buildSelect(s).toString))
      query.lim.foreach(l => sb.append(" limit " + l))
      query.sk.foreach(s => sb.append(" skip " + s))
      modify.foreach(m => sb.append(" modify with " + buildModify(m)))
      sb.toString
    }

    def buildSignature[R, M <: MongoRecord[M]](query: BaseQuery[M, R, _, _, _, _]): String = {
      val sb = new StringBuilder
      sb.append(buildCondition(query.condition, signature = true).toString)
      query.order.foreach(o => sb.append(" order by " + buildOrder(o).toString))
      sb.toString
    }
  }

  object QueryExecutor {

    import QueryHelpers._
    import MongoHelpers.MongoBuilder._
    
    private[rogue] def runCommand[T](description: => String, id: MongoIdentifier)(f: => T): T = runCommand(description, id.toString)(f)
    
    private[rogue] def runCommand[T](description: => String, id: String)(f: => T): T = {
      val start = System.currentTimeMillis 
      try {
        f
      } catch {
        case e: Exception =>
          throw new RogueException("Mongo query on %s [%s] failed after %d ms".format(id, description, System.currentTimeMillis - start), e)
      } finally {
        logger.log(description, System.currentTimeMillis - start)
      }
    }

    def condition[M <: MongoRecord[M], T](operation: String,
                                            query: BaseQuery[M, _, _, _, _, _])
                                           (f: DBObject => T): T = {

      validator.validateQuery(query)
      val collection = query.meta.collectionName
      val cnd = buildCondition(query.condition)
      
      def description = "Mongo %s.%s (%s)" format (collection, operation, cnd)
      
      runCommand(description, query.meta.mongoIdentifier){
        f(cnd)
      }
    }

    def modify[M <: MongoRecord[M], T](operation: String,
                                         mod: BaseModifyQuery[M])
                                        (f: (DBObject, DBObject) => T): Unit = {
      validator.validateModify(mod)
      if (!mod.mod.clauses.isEmpty) {

        val collection = mod.query.meta.collectionName
        val q = buildCondition(mod.query.condition)
        val m = buildModify(mod.mod)
        
        def description = "Mongo %s.%s (%s, %s)" format (collection, operation, q, m)
        
        runCommand(description, mod.query.meta.mongoIdentifier){
          f(q, m)
        }
      }
    }

    def query[M <: MongoRecord[M]](operation: String,
                                   query: BaseQuery[M, _, _, _, _, _],
                                   batchSize: Option[Int])
                                  (f: DBObject => Unit): Unit = {
      doQuery(operation, query){cursor => 
        batchSize.foreach(cursor batchSize _)
        while (cursor.hasNext)
          f(cursor.next)
      }
    }

    def explain[M <: MongoRecord[M]](operation: String,
                                     query: BaseQuery[M, _, _, _, _, _]): String = {
      var explanation = ""
      doQuery(operation, query){cursor =>
        explanation += cursor.explain.toString
      }
      explanation
    }

    private[rogue] def doQuery[M <: MongoRecord[M]](operation: String,
                                   query: BaseQuery[M, _, _, _, _, _])
                                  (f: DBCursor  => Unit): Unit = {
      
      validator.validateQuery(query)
      val collection = query.meta.collectionName
      val cnd = buildCondition(query.condition)
      val ord = query.order.map(buildOrder)
      val sel = query.select.map(buildSelect) getOrElse buildSelectFromNames(query.meta.metaFields.view.map(_.name))
      
      def description = {
        val str = new StringBuilder("Mongo " + collection +"." + operation)
        str.append("("+cnd)
        str.append(", "+sel)
        str.append(")")
        ord.foreach(o => str.append(".sort("+o+")"))
        query.sk.foreach(sk => str.append(".skip("+sk+")"))
        query.lim.foreach(l => str.append(".limit("+l+")"))
        query.maxScan.foreach(m => str.append("._addSpecial(\"$maxScan\", "+m+")"))
        query.comment.foreach(c => str.append("._addSpecial(\"$comment\", \""+c+"\")"))
        query.hint.foreach(h => str.append(".hint("+h+")"))
        str.toString
      }
      
      runCommand(description, query.meta.mongoIdentifier){
        MongoDB.useCollection(query.meta.mongoIdentifier, query.meta.collectionName) { coll =>

          lazy val empty = BasicDBObjectBuilder.start.get

          val cursor = coll.find(cnd, sel).limit(query.lim getOrElse 0).skip(query.sk getOrElse 0)
          ord.foreach(cursor sort _)
          query.maxScan.foreach(cursor addSpecial("$maxScan", _))
          query.comment.foreach(cursor addSpecial("$comment", _))
          query.hint.foreach(cursor hint(_))
          f(cursor)
        }
      }
    }
  }
}
