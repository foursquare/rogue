// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

class QueryOptimizer {
  def isEmptyClause(clause: QueryClause[_]): Boolean = clause match {
    case AllQueryClause(_, vs, _) => vs.isEmpty
    case InQueryClause(_, vs, _) => vs.isEmpty
    case EmptyQueryClause(_, _) => true
    case _ => false
  }

  def isEmptyQuery(query: BaseQuery[_, _, _, _, _, _, _]): Boolean = {
    query.condition.clauses.exists(isEmptyClause)
  }

  def isEmptyQuery(query: BaseModifyQuery[_]): Boolean =
    isEmptyQuery(query.query)

  def isEmptyQuery(query: BaseFindAndModifyQuery[_, _]): Boolean =
    isEmptyQuery(query.query)
}
