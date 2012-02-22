// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

// ***************************************************************************
// *** Phantom types
// ***************************************************************************

abstract sealed class MaybeOrdered
abstract sealed class Ordered extends MaybeOrdered
abstract sealed class Unordered extends MaybeOrdered

abstract sealed class MaybeSelected
abstract sealed class Selected extends MaybeSelected
abstract sealed class Unselected extends MaybeSelected

abstract sealed class MaybeLimited
abstract sealed class Limited extends MaybeLimited
abstract sealed class Unlimited extends MaybeLimited

abstract sealed class MaybeSkipped
abstract sealed class Skipped extends MaybeSkipped
abstract sealed class Unskipped extends MaybeSkipped

abstract sealed class MaybeHasOrClause
abstract sealed class HasOrClause extends MaybeHasOrClause
abstract sealed class HasNoOrClause extends MaybeHasOrClause

sealed trait MaybeIndexed
sealed trait Indexable extends MaybeIndexed
sealed trait IndexScannable extends MaybeIndexed

abstract sealed class NoIndexInfo extends Indexable with IndexScannable
abstract sealed class Index extends Indexable with IndexScannable
abstract sealed class PartialIndexScan extends IndexScannable
abstract sealed class IndexScan extends IndexScannable
abstract sealed class DocumentScan extends MaybeIndexed

case object NoIndexInfo extends NoIndexInfo
case object Index extends Index
case object PartialIndexScan extends PartialIndexScan
case object IndexScan extends IndexScan
case object DocumentScan extends DocumentScan

abstract sealed class MaybeUsedIndex
abstract sealed class UsedIndex extends MaybeUsedIndex
abstract sealed class HasntUsedIndex extends MaybeUsedIndex
