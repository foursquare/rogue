// // Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

// package com.foursquare.index

// import com.foursquare.rogue.MongoHelpers.AndCondition
// import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
// import net.liftweb.record.Field

// // ***************************************************************************
// // *** Indexes
// // ***************************************************************************

// class IndexEnforcerBuilder[M <: MongoRecord[M]](meta: M with MongoMetaRecord[M] with IndexedRecord[M]) {
//   type MetaM = M with MongoMetaRecord[M] with IndexedRecord[M]

//   def useIndex[F1 <: Field[_, M]](i: MongoIndex1[M, F1, _]): IndexEnforcer1[M, NoIndexInfo, F1, HasntUsedIndex] = {
//     new IndexEnforcer1[M, NoIndexInfo, F1, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None, None))
//   }

//   def useIndex[F1 <: Field[_, M], F2 <: Field[_, M]](i: MongoIndex2[M, F1, _, F2, _]): IndexEnforcer2[M, NoIndexInfo, F1, F2, HasntUsedIndex] = {
//     new IndexEnforcer2[M, NoIndexInfo, F1, F2, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None, None))
//   }

//   def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M]](i: MongoIndex3[M, F1, _, F2, _, F3, _]): IndexEnforcer3[M, NoIndexInfo, F1, F2, F3, HasntUsedIndex] = {
//     new IndexEnforcer3[M, NoIndexInfo, F1, F2, F3, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None, None))
//   }

//   def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M], F4 <: Field[_, M]](i: MongoIndex4[M, F1, _, F2, _, F3, _, F4, _]): IndexEnforcer4[M, NoIndexInfo, F1, F2, F3, F4, HasntUsedIndex] = {
//     new IndexEnforcer4[M, NoIndexInfo, F1, F2, F3, F4, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None, None))
//   }

//   def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M], F4 <: Field[_, M], F5 <: Field[_, M]](i: MongoIndex5[M, F1, _, F2, _, F3, _, F4, _, F5, _]): IndexEnforcer5[M, NoIndexInfo, F1, F2, F3, F4, F5, HasntUsedIndex] = {
//     new IndexEnforcer5[M, NoIndexInfo, F1, F2, F3, F4, F5, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None, None))
//   }

//   def useIndex[F1 <: Field[_, M], F2 <: Field[_, M], F3 <: Field[_, M], F4 <: Field[_, M], F5 <: Field[_, M], F6 <: Field[_, M]](i: MongoIndex6[M, F1, _, F2, _, F3, _, F4, _, F5, _, F6, _]): IndexEnforcer6[M, NoIndexInfo, F1, F2, F3, F4, F5, F6, HasntUsedIndex] = {
//     new IndexEnforcer6[M, NoIndexInfo, F1, F2, F3, F4, F5, F6, HasntUsedIndex](meta, new BaseQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause](meta, None, None, None, None, None, AndCondition(Nil, None), None, None, None))
//   }
// }

// case class IndexEnforcer1[M <: MongoRecord[M],
//                           Ind <: MaybeIndexed,
//                           F1 <: Field[_, M],
//                           UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
//                                              q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
//   def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
//     q.where(_ => clause(f1Func(meta)))
//   }

//   def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
//     q.and(_ => clause(f1Func(meta)))
//   }

//   def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = {
//     q.iscan(_ => clause(f1Func(meta)))
//   }

//   def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
// }

// case class IndexEnforcer2[M <: MongoRecord[M],
//                           Ind <: MaybeIndexed,
//                           F1 <: Field[_, M],
//                           F2 <: Field[_, M],
//                           UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
//                                                           q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
//   def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer1[M, Index, F2, UsedIndex] = {
//     new IndexEnforcer1[M, Index, F2, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
//   }

//   def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer1[M, Index, F2, UsedIndex] = {
//     new IndexEnforcer1[M, Index, F2, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
//   }

//   def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer1[M, IndexScan, F2, UsedIndex] = {
//     new IndexEnforcer1[M, IndexScan, F2, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
//   }

//   def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F2, UsedIndex] = {
//     new IndexEnforcer1[M, IndexScan, F2, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
// }

// case class IndexEnforcer3[M <: MongoRecord[M],
//                           Ind <: MaybeIndexed,
//                           F1 <: Field[_, M],
//                           F2 <: Field[_, M],
//                           F3 <: Field[_, M],
//                           UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
//                                              q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
//   def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer2[M, Index, F2, F3, UsedIndex] = {
//     new IndexEnforcer2[M, Index, F2, F3, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
//   }

//   def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer2[M, Index, F2, F3, UsedIndex] = {
//     new IndexEnforcer2[M, Index, F2, F3, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
//   }

//   def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex] = {
//     new IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
//   }

//   def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex] = {
//     new IndexEnforcer2[M, IndexScan, F2, F3, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F3, UsedIndex] = {
//     new IndexEnforcer1[M, IndexScan, F3, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
// }

// case class IndexEnforcer4[M <: MongoRecord[M],
//                           Ind <: MaybeIndexed,
//                           F1 <: Field[_, M],
//                           F2 <: Field[_, M],
//                           F3 <: Field[_, M],
//                           F4 <: Field[_, M],
//                           UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
//                                                            q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
//   def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex] = {
//     new IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
//   }

//   def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex] = {
//     new IndexEnforcer3[M, Index, F2, F3, F4, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
//   }

//   def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex] = {
//     new IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
//   }

//   def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex] = {
//     new IndexEnforcer3[M, IndexScan, F2, F3, F4, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer2[M, IndexScan, F3, F4, UsedIndex] = {
//     new IndexEnforcer2[M, IndexScan, F3, F4, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F4, UsedIndex] = {
//     new IndexEnforcer1[M, IndexScan, F4, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3, f4Func: M => F4)(implicit ev: UsedInd <:< UsedIndex): AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause] = q
// }

// case class IndexEnforcer5[M <: MongoRecord[M],
//                           Ind <: MaybeIndexed,
//                           F1 <: Field[_, M],
//                           F2 <: Field[_, M],
//                           F3 <: Field[_, M],
//                           F4 <: Field[_, M],
//                           F5 <: Field[_, M],
//                           UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
//                                                            q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
//   def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex] = {
//     new IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
//   }

//   def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex] = {
//     new IndexEnforcer4[M, Index, F2, F3, F4, F5, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
//   }

//   def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex] = {
//     new IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
//   }

//   def rangeScan(f1Func: M => F1)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex] = {
//     new IndexEnforcer4[M, IndexScan, F2, F3, F4, F5, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer3[M, IndexScan, F3, F4, F5, UsedIndex] = {
//     new IndexEnforcer3[M, IndexScan, F3, F4, F5, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer2[M, IndexScan, F4, F5, UsedIndex] = {
//     new IndexEnforcer2[M, IndexScan, F4, F5, UsedIndex](meta, q)
//   }

//   def rangeScan(f1Func: M => F1, f2Func: M => F2, f3Func: M => F3, f4Func: M => F4)(implicit ev: UsedInd <:< UsedIndex): IndexEnforcer1[M, IndexScan, F5, UsedIndex] = {
//     new IndexEnforcer1[M, IndexScan, F5, UsedIndex](meta, q)
//   }
// }

// case class IndexEnforcer6[M <: MongoRecord[M],
//                           Ind <: MaybeIndexed,
//                           F1 <: Field[_, M],
//                           F2 <: Field[_, M],
//                           F3 <: Field[_, M],
//                           F4 <: Field[_, M],
//                           F5 <: Field[_, M],
//                           F6 <: Field[_, M],
//                           UsedInd <: MaybeUsedIndex](meta: M with MongoMetaRecord[M],
//                                                           q: AbstractQuery[M, M, Unordered, Unselected, Unlimited, Unskipped, HasNoOrClause]) {
//   def where[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex] = {
//     new IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex](meta, q.where(_ => clause(f1Func(meta))))
//   }

//   def and[F, ClauseInd <: Indexable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd])(implicit ev: Ind <:< Indexable): IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex] = {
//     new IndexEnforcer5[M, Index, F2, F3, F4, F5, F6, UsedIndex](meta, q.and(_ => clause(f1Func(meta))))
//   }

//   def iscan[F, ClauseInd <: IndexScannable](f1Func: M => F1)(clause: F1 => IndexableQueryClause[F, ClauseInd]): IndexEnforcer5[M, IndexScan, F2, F3, F4, F5, F6, UsedIndex] = {
//     new IndexEnforcer5[M, IndexScan, F2, F3, F4, F5, F6, UsedIndex](meta, q.iscan(_ => clause(f1Func(meta))))
//   }

//   // IndexEnforcer6 doesn't have methods to scan any later fields in the index
//   // because there's no way that we got here via an iscan on a bigger index --
//   // there are no bigger indexes. We require that the first column on the index
//   // gets used.
// }

