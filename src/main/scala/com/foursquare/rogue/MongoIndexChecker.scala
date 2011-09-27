// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.rogue

import scala.collection.immutable.ListMap
import net.liftweb.common.Loggable
import net.liftweb.mongodb.record._
import net.liftweb.util.Props

trait MongoQueryTypes {
  type GenericQuery[M <: MongoRecord[M], R] = AbstractQuery[M, R, _, _, _, _, _]
  type GenericBaseQuery[M <: MongoRecord[M], R] = BaseQuery[M, R, _, _, _, _, _]
}

object MongoQueryNormalizer extends Loggable with MongoQueryTypes {
  /**
   * Flattens an arbitrary query into DNF - that is, into a list of query alternatives
   * implicitly joined by logical "or", where each of alternatives consists of a list of query
   * clauses implicitly joined by "and".
   */
  def flattenCondition(condition: MongoHelpers.AndCondition): List[List[QueryClause[_]]] = {
    condition.orCondition match {
      case None => List(condition.clauses)
      case Some(or) => for {
        subconditions <- or.conditions
        subclauses <- flattenCondition(subconditions)
      } yield (condition.clauses ++ subclauses)
    }
  }

  def normalizeCondition(condition: MongoHelpers.AndCondition): List[List[QueryClause[_]]] = {
    flattenCondition(condition).map(_.filter(_.expectedIndexBehavior != IndexBehavior.DocumentScan))
  }

}

object MongoIndexChecker extends Loggable with MongoQueryTypes {

  /**
   * Verifies that the indexes expected for a query actually exist in the mongo database.
   * Signals an error if the indexes don't fulfull the expectations.
   * @param query the query being validated.
   * @param conditions the query converted to DNF (e.g., by flattenCondition)
   */
  def validateIndexExpectations(query: GenericBaseQuery[_, _],
                                conditions: List[List[QueryClause[_]]]) = {
    conditions.forall(clauses => {
      clauses.forall(clause => {
        // DocumentScan expectations have been filtered out at this point.
        // We just have to worry about expectations being more optimistic than actual.
        val badExpectations = List(
          IndexBehavior.Index -> List(IndexBehavior.PartialIndexScan, IndexBehavior.IndexScan,IndexBehavior.DocumentScan),
          IndexBehavior.IndexScan -> List(IndexBehavior.DocumentScan)
        )
        badExpectations.forall{ case (expectation, badActual) => {
          if (clause.expectedIndexBehavior == expectation &&
              badActual.exists(_ == clause.actualIndexBehavior)) {
            false
            throw new Exception(
                "Query is expecting %s on %s but actual behavior is %s. query = %s" format
                (clause.expectedIndexBehavior, clause.fieldName, clause.actualIndexBehavior, query.toString))
          } else true
        }}
      })
    })
  }

  /**
   * Verifies that the index expected by a query both exists, and will be used by MongoDB
   * to execute that query. (Due to vagaries of the MongoDB implementation, sometimes a
   * conceptually usable index won't be found.)
   * @param query the query
   * @param indexes the list of indexes that exist in the database
   * @param the query clauses in DNF form.
   */
  def validateQueryMatchesSomeIndex(query: GenericBaseQuery[_, _],
                                    indexes: List[ListMap[String, Any]],
                                    conditions: List[List[QueryClause[_]]]) = {
    lazy val indexString = indexes.map(idx => "{%s}".format(idx.map(fld => "%s:%s".format(fld._1, fld._2)).mkString(", "))).mkString(", ")
    conditions.forall(clauses => {
      clauses.isEmpty || matchesUniqueIndex(clauses) || indexes.exists(idx => matchesIndex(idx.keys.toList, clauses)) ||
      signalError("Query does not match an index! query: %s, indexes: %s" format (
        query.toString, indexString))
    })
  }

  private def matchesUniqueIndex(clauses: List[QueryClause[_]]) = {
    // Special case for overspecified queries matching on the _id field.
    // TODO: Do the same for any overspecified query exactly matching a unique index.
    clauses.exists(clause => clause.fieldName == "_id" && clause.actualIndexBehavior == IndexBehavior.Index)
  }

  private def matchesIndex(index: List[String],
                           clauses: List[QueryClause[_]]) = {
    // Unless explicitly hinted, MongoDB will only use an index if the first
    // field in the index matches some query field.
    clauses.exists(_.fieldName == index.head) &&
        matchesCompoundIndex(index, clauses, scanning = false)
  }

  /** 
   * Matches a compound index against a list of query clauses, verifying that each
   * query clause has its index expectations matched by a field of the index.
   * @param index the index to be checked.
   * @param clauses a list of query clauses joined by logical "and".
   * @return true if every clause of the query is matched by a field of the
   *   index; false otherwise.
   */
  private def matchesCompoundIndex(index: List[String],
                                   clauses: List[QueryClause[_]], scanning: Boolean): Boolean = {
    if (clauses.isEmpty) {
      // All of the clauses have been matched to an index field. We are done!
      true
    } else {
      index match {
        case Nil => {
          // Oh no! The index is exhausted but we still have clauses to match.
          false
        }
        case field :: rest => {
          val (matchingClauses, remainingClauses) = clauses.partition(_.fieldName == field)
          matchingClauses match {
            case matchingClause :: _ => {
              // If a previous field caused a scan, this field must scan too.
              val expectationOk = !scanning || matchingClause.expectedIndexBehavior == IndexBehavior.IndexScan
              // If this field causes a scan, later fields must scan too.
              val nowScanning = scanning ||
                  matchingClause.actualIndexBehavior == IndexBehavior.IndexScan ||
                  matchingClause.actualIndexBehavior == IndexBehavior.PartialIndexScan
              expectationOk && matchesCompoundIndex(rest, remainingClauses, scanning = nowScanning)
            }
            case Nil => {
              // We can skip a field in the index, but everything after it must scan.
              matchesCompoundIndex(rest, remainingClauses, scanning = true)
            }
          }
        }
      }
    }
  }

  /**
   * Utility method that allows us to signal an error from inside of a disjunctive expression.
   * e.g., "blah || blah || black || signalError(....)".
   *
   * @param msg a message string describing the error.
   */
  private def signalError(msg : String) : Boolean = {
    throw new Exception(msg)
    false
  }

}
