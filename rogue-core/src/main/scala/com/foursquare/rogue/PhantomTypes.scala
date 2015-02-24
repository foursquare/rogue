// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import scala.annotation.implicitNotFound

// ***************************************************************************
// *** Phantom types
// ***************************************************************************

sealed trait Ordered
sealed trait Unordered
sealed trait Ord extends Ordered with Unordered

sealed trait Selected
sealed trait SelectedOne extends Selected
sealed trait Unselected
sealed trait Sel extends Selected with SelectedOne with Unselected

sealed trait Limited
sealed trait Unlimited
sealed trait Lim extends Limited with Unlimited

sealed trait Skipped
sealed trait Unskipped
sealed trait Sk extends Skipped with Unskipped

sealed trait HasOrClause
sealed trait HasNoOrClause
sealed trait Or extends HasOrClause with HasNoOrClause

sealed trait ShardKeyNotSpecified
sealed trait ShardAware
sealed trait ShardKeySpecified extends ShardAware
sealed trait AllShardsOk extends ShardAware
sealed trait Sh extends ShardKeyNotSpecified with ShardKeySpecified with AllShardsOk

sealed trait HasSearchClause
sealed trait HasNoSearchClause
sealed trait Sr extends HasSearchClause with HasNoSearchClause

@implicitNotFound(msg = "Query must be Unordered, but it's actually ${In}")
trait AddOrder[-In, +Out] extends Required[In, Unordered]
object AddOrder {
  implicit def addOrder[Rest >: Sel with Lim with Sk with Or with Sh with Sr]: AddOrder[Rest with Unordered, Rest with Ordered] = null
}

@implicitNotFound(msg = "Query must be Unordered with HasNoSearchClause, but it's actually ${In}")
trait AddNaturalOrder[-In, +Out] extends Required[In, Unordered with HasNoSearchClause]
object AddNaturalOrder {
  implicit def addOrder[Rest >: Sel with Lim with Sk with Or with Sh with Sr]: AddNaturalOrder[Rest with Unordered with HasNoSearchClause, Rest with Ordered with HasNoSearchClause] = null
}

@implicitNotFound(msg = "Query must be Unordered with HasSearchClause, but it's actually ${In}")
trait AddScoreOrder[-In, +Out] extends Required[In, Unordered with HasSearchClause]
object AddScoreOrder {
  implicit def addScoreOrder[Rest >: Sel with Lim with Sk with Or with Sh with Sr]: AddScoreOrder[Rest with Unordered with HasSearchClause, Rest with Ordered with HasSearchClause] = null
}

@implicitNotFound(msg = "Query must be Unselected, but it's actually ${In}")
trait AddSelect[-In, +Out, +One] extends Required[In, Unselected]
object AddSelect {
  implicit def addSelect[Rest >: Ord with Lim with Sk with Or with Sh with Sr]: AddSelect[Rest with Unselected, Rest with Selected, Rest with SelectedOne] = null
}

@implicitNotFound(msg = "Query must be Unlimited, but it's actually ${In}")
trait AddLimit[-In, +Out] extends Required[In, Unlimited]
object AddLimit {
  implicit def addLimit[Rest >: Ord with Sel with Sk with Or with Sh with Sr]: AddLimit[Rest with Unlimited, Rest with Limited] = null
}

@implicitNotFound(msg = "Query must be Unskipped, but it's actually ${In}")
trait AddSkip[-In, +Out] extends Required[In, Unskipped]
object AddSkip {
  implicit def addSkip[Rest >: Ord with Sel with Lim with Or with Sh with Sr]: AddSkip[Rest with Unskipped, Rest with Skipped] = null
}

@implicitNotFound(msg = "Query must be HasNoSearchClause, but it's actually ${In}")
trait AddText[-In, +Out] extends Required[In, HasNoSearchClause]
object AddText {
  implicit def addText[Rest >: Ord with Sel with Lim with Sk with Or with Sh]: AddText[Rest with HasNoSearchClause, Rest with HasSearchClause] = null
}

@implicitNotFound(msg = "Query must be HasNoOrClause, but it's actually ${In}")
trait AddOrClause[-In, +Out] extends Required[In, HasNoOrClause]
object AddOrClause {
  implicit def addOrClause[Rest >: Ord with Sel with Lim with Sk with Sh with Sr]: AddOrClause[Rest with HasNoOrClause, Rest with HasOrClause] = null
}

trait AddShardAware[-In, +Specified, +AllOk] extends Required[In, ShardKeyNotSpecified]
object AddShardAware {
  implicit def addShardAware[Rest >: Ord with Sel with Lim with Sk with Or with Sr]: AddShardAware[Rest with ShardKeyNotSpecified, Rest with ShardKeySpecified, Rest with AllShardsOk] = null
}

@implicitNotFound(msg = "In order to call this method, ${A} must NOT be a subclass of ${B}.")
sealed trait !<:<[A, B]
object !<:< {
  implicit def any[A, B]: A !<:< B = null
  implicit def sub1[A, B >: A]: A !<:< B = null
  implicit def sub2[A, B >: A]: A !<:< B = null
}

@implicitNotFound(msg = "Cannot prove that ${A} <: ${B}")
class Required[-A, +B] {
  def apply[M, R](q: Query[M, R, A]): Query[M, R, B] = q.asInstanceOf[Query[M, R, B]]
}
object Required {
  val default = new Required[Any, Any]
  implicit def conforms[A]: Required[A, A] = default.asInstanceOf[Required[A, A]]
}

@implicitNotFound(msg = "${M} is a sharded collection but the shard key is not specified. Either specify the shard key or add `.allShards` to the query.")
trait ShardingOk[M, -S]
object ShardingOk {
  implicit def sharded[M <: Sharded, Sh <: ShardAware]: ShardingOk[M, Sh] = null
  implicit def unsharded[M, State](implicit ev: M !<:< Sharded): ShardingOk[M, State] = null
}

@implicitNotFound(msg = "${M} is a sharded collection. Either specify the shard key or use `.updateMulti()`.")
trait RequireShardKey[M, -S]
object RequireShardKey {
  implicit def sharded[M <: Sharded, Sh <: ShardKeySpecified]: RequireShardKey[M, Sh] = null
  implicit def unsharded[M, State](implicit ev: M !<:< Sharded): RequireShardKey[M, State] = null
}


sealed trait MaybeIndexed
sealed trait Indexable extends MaybeIndexed
sealed trait IndexScannable extends MaybeIndexed

sealed trait NoIndexInfo extends Indexable with IndexScannable
sealed trait Index extends Indexable with IndexScannable
sealed trait PartialIndexScan extends IndexScannable
sealed trait IndexScan extends IndexScannable
sealed trait DocumentScan extends MaybeIndexed
sealed trait TextIndex extends Indexable

case object NoIndexInfo extends NoIndexInfo
case object Index extends Index
case object PartialIndexScan extends PartialIndexScan
case object IndexScan extends IndexScan
case object DocumentScan extends DocumentScan
case object TextIndex extends TextIndex

sealed trait MaybeUsedIndex
sealed trait UsedIndex extends MaybeUsedIndex
sealed trait HasntUsedIndex extends MaybeUsedIndex
