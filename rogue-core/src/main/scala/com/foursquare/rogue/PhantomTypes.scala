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

@implicitNotFound(msg = "Query must be Unordered, but it's actually ${In}")
class AddOrder[-In, +Out]
@implicitNotFound(msg = "Query must be Unselected, but it's actually ${In}")
class AddSelect[-In, +Out, +One]
@implicitNotFound(msg = "Query must be Unlimited, but it's actually ${In}")
class AddLimit[-In, +Out]
@implicitNotFound(msg = "Query must be Unskipped, but it's actually ${In}")
class AddSkip[-In, +Out]
@implicitNotFound(msg = "Query must be HasNoOrClause, but it's actually ${In}")
class AddOrClause[-In, +Out]


sealed trait MaybeIndexed
sealed trait Indexable extends MaybeIndexed
sealed trait IndexScannable extends MaybeIndexed

sealed trait NoIndexInfo extends Indexable with IndexScannable
sealed trait Index extends Indexable with IndexScannable
sealed trait PartialIndexScan extends IndexScannable
sealed trait IndexScan extends IndexScannable
sealed trait DocumentScan extends MaybeIndexed

case object NoIndexInfo extends NoIndexInfo
case object Index extends Index
case object PartialIndexScan extends PartialIndexScan
case object IndexScan extends IndexScan
case object DocumentScan extends DocumentScan

sealed trait MaybeUsedIndex
sealed trait UsedIndex extends MaybeUsedIndex
sealed trait HasntUsedIndex extends MaybeUsedIndex
