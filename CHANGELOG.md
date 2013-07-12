## Release Notes

# 2.2.0

- ObjectIdKey trait to replace deprecated ObjectId
- overload inc method to take a Double (djonmez)

# 2.1.0

- fixed edge case rendering $or queries (thanks Konrad!)
- support for user defined setFromAny in fields with BsonRecordField and iteratees (thanks Arseny!)
- $elemMatch support
- simplified pullObjectWhere notation
- $nearSphere support
- support for $ in .select() statements
- better support for comparing DateFields against Dates and DateTimes

# 2.0.0

- StringQueryField support for subtypes of String

# 2.0.0-RC4

- split off index code into rogue-index

# 2.0.0-RC3

- long subtypes are BSONTypes

# 2.0.0-RC2

- add BSONType type class to eliminate runtime serialization errors

# 2.0.0-RC1

- support for type-safe ID fields

# 2.0.0-beta22

- support for scala 2.9.2 and 2.10.0 (mattpap)
- support for Model.distinct(_.field) (mattpap)
- sbt 0.12.0 (mattpap)

# 2.0.0-beta21

- fix signatures for $or queries

# 2.0.0-beta20

- hook up readPreference, remove notion of defaultReadPreference

# 2.0.0-beta19

- fixed bug in $or clause serialization
- support for DateTime fields
- defaultReadPreference should be 'secondary'

# 2.0.0-beta18

- setTo overload takes an Option (setTo None == unset)
- fixed some implicit conversions

# 2.0.0-beta17

- $slice support
- list neqs
- updated Indexing.md

# 2.0.0-beta15

- allow Date and List fields to act like any other field (eqs, etc)

# 2.0.0-beta13

- Index checker: don't validate indexes if there are no indexes

# 2.0.0-beta12

- report cluster name to QueryLogger

# 2.0.0-beta11

- fixed bug around count() respecting skip and limit
- added QueryLogger.onExecuteQuery callback

# 2.0.0-beta10

- Move index classes to com.foursquare.rogue.index
- Add SeqQueryField and SeqModifyField

# 2.0.0-beta9

- Replace Box with Option in rogue-core

# 2.0.0-beta8

- Make ObjectIdQueryField more generic

# 2.0.0-beta7

- revert change where we always pass a negative limit
- fix weird interaction between negative limits and batchSize

# 2.0.0-beta6

- simplified phantom types
- use size() instead of count() to respect skip and limit
- shardkey awareness
- lots of renames, most notably AbstractQuery and ModifyQuery => Query
- pass a negative number to DBCursor.limit() so that the cursor closes

# 2.0.0-beta5

- Internal: Use standard Either convention of failure on the Left.
- Use readPreference instead of slaveOk
- Remove generic Field[V, M] => QueryField[V, M] implicit

# 2.0.0-beta4

- split off com.foursquare.field into a standalone project

# 2.0.0-beta3

- upgrade to v0.6 of gpg plugin
- make QueryExecutor a trait
- rename SelectField#apply so implicits to SelectField don't cause trouble

# 2.0.0-beta2

- fix O(N^2) bug in fetchBatch and iterateBatch

# 2.0.0-beta1

- total refactor
- separate query building from query execution
- break out lift support into rogue-lift
- core of rogue now in rogue-core, agnostic to model representation
- drop support for scala 2.8.x

# 1.1.6

- iteratee support
- default WriteConcern is configurable
- renamed blockingBulkDelete_!! to bulkDelete_!! (takes a WriteConcern)
- moved gt, lt into base QueryField (nsanch)
- fixed the way nested subfield queries work if both fields are ListFields

# 1.1.5

- fixed handling of subfields of list fields
- allow nested subfields for BsonRecordFields

# 1.1.4

- removed EmptyQuery, fixed handling of upserts on empty queries
- BaseQuery.asDBObject, BaseModifyQuery.asDBObject
- fix for subselecting when the top-level field doesn't exist (nsanch)
- fixes for publishing to sonatype
- bumped mongo java driver version to 2.7.3

# 1.1.3

- fixed bug where findAndModify upsert with returnNew = false was returning Some
- fixed bug where $regex query on a field would not allow other queries on that field
- publishing to sonatype instead of scala-tools

# 1.1.2

- allow $or queries in modify commands

# 1.1.1

- select/selectCase up to 10 fields (davidtaylor)
- only validate lists on $all and $in queries (jliszka)
- pass query object to logging hook (jliszka)

# 1.1.0

- compile-time index checking (nsanch)
- stop building select clause from all fields (jliszka)
- QueryLogger.logIndexHit hook (jliszka)
- use distinct values in $in and $all queries (jliszka)
- slaveOk query modifier (nsanch)

# 1.0.29

- updated inline documentation (markcc)
- between takes a tuple (davidt)
- end-to-end tests (nsanch)
- subfield select on embedded list (nsanch)
- regex match operator for string fields (jliszka)

# 1.0.28

- Support for the $ positional operator
- pullWhere - $pull by query instead of exact match

# 1.0.27

- Mongo index checking (see [here](https://github.com/foursquare/rogue/blob/master/docs/Indexing.md) for documentation)

# 1.0.26

- $rename support

# 1.0.25

- ability to supply a WriteConcern to updateOne, updateMulti and upsertOne.
- select and selectCase can handle 7 and 8 parameters

# 1.0.24

- $bit support

# 1.0.23

- Add hook for intercepting and transforming queries right before sending request to mongodb.

# 1.0.22

- improved support for subfield queries on BsonRecordField

# 1.0.21

- support for subfield queries on BsonRecordField
- added "matches" operator (for regexes) on StringFields with explicit index behavior expectations
- fixed some more broken logging

# 1.0.20

- sbt 0.10.0
- raw access do BasicDBObjectBuilder in query builder
- fixed some broken logging

# 1.0.19

- whereOpt support: Venue.whereOpt(uidOpt)(_.userid eqs _)
- Pass the query signature to the logging hook

# 1.0.18

- findAndModify support
- $or query support
- efficient .exists query method (thanks Jorge!)
- support for BsonRecordField and BsonRecordListField (thanks Marc!)
- type-safe foreign key condtions, e.g., Tip.where(_.venueid eqs venueObj) (thanks dtaylor!)

# 1.0.17

- blockingBulkDelete_!! which takes a WriteConcern
- more uniform query logging

# 1.0.16

- skipOpt query modifier
- use built-in interpreter for type checking tests

# 1.0.15

- .toString produces runnable javascript commands for mongodb console
- added tests for constructions that should not compile
- selectCase() builder method for select()ing via case class
- support for $nin (nin) and $ne (notcontains) on list fields
- unchecked warnings cleanup

# 1.0.14:

- index hinting support
- support for selecting subfields (MongoMapField and MongoCaseClassField only; no support for MongoCaseClassListField)
- "between" convenience operator (numeric)
- scala 2.9.0 and 2.9.0-1 build support -- thanks eltimn!

# 1.0.13:

- fixed ObjectId construction for date ranges by zeroing out machine, pid and counter fields
- support for $maxScan and $comment addSpecial parameters on find() queries

# 1.0.12:

- always specify field names to return in the query; if select() was not specified, use all field names from the model
- some code cleanup (use case classes and copy() to save some typing)

# 1.0.11:

- explain() method on BaseQuery (thanks tjulien!)
- support for select()ing up to 6 fields

# 1.0.10:

- regression fix for 1.0.9

# 1.0.9

- added hooks for full query validation
- support for $type and $mod query operators
- query signatures: string version of a query without values
- support for indicating when a query clause is intended to hit an index (for runtime index checking, if you wish to implement it)

# 1.0.8

- extra logging around mongo exceptions

# 1.0.7

- support for empty (noop) modify queries

# 1.0.6

- fetchBatch now uses db cursors
- building against Lift snapshot (thanks Indrajit!)
- support for crossbuilding 2.8.0 & 2.8.1

# 1.0.5

- added tiny bit more type safety to unsafeField subfield selector

# 1.0.4

- bug fix: alwasy set _id in select() queries

# 1.0.3

- fixed setTo serialization
- eqs/neqs support for GeoQueryField

# 1.0.2

- support for querying sub-fields of a map

# 1.0.1

- added BasePaginatedQuery.setCountPerPage()

