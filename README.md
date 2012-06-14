# Rogue

Rogue is a type-safe internal Scala DSL for constructing and executing find and modify commands against
MongoDB in the Lift web framework. It is fully expressive with respect to the basic options provided
by MongoDB's native query language, but in a type-safe manner, building on the record types specified in 
your Lift models. An example:

    Venue where (_.mayor eqs 1234) and (_.categories contains "Thai") fetch(10)

The type system enforces the following constraints:

- the fields must actually belong to the record (e.g., mayor is a field on the Venue record)
- the field type must match the operand type (e.g., mayor is an IntField)
- the operator must make sense for the field type (e.g., categories is a MongoListField[String])

In addition, the type system ensures that certain builder methods are only used in certain circumstances.
For example, take this more complex query:

    Venue where (_.closed eqs false) orderAsc(_.popularity) limit(10) modify (_.closed setTo true) updateMulti

This query purportedly finds the 10 least popular open venues and closes them. However, MongoDB
does not (currently) allow you to specify limits on modify queries, so Rogue won't let you either.
The above will generate a compiler error. 

Constructions like this:

    def myMayorships = Venue where (_.mayor eqs 1234) limit(5)
    ...
    myMayorships.fetch(10)

will also not compile, here because a limit is being specified twice. Other similar constraints
are in place to prevent you from accidentally doing things you don't want to do anyway.

## Installation

Because Rogue is designed to work with several versions of lift-mongodb-record (2.2, 2.3, 2.4),
you'll want to declare your dependency on Rogue as `intransitive` and declare an explicit dependency
on the version of Lift you want to target. In sbt, that would look like the following: 

    val rogueField      = "com.foursquare" %% "rogue-field"         % "2.0.0-beta7" intransitive()
    val rogueCore       = "com.foursquare" %% "rogue-core"          % "2.0.0-beta7" intransitive()
    val rogueLift       = "com.foursquare" %% "rogue-lift"          % "2.0.0-beta7" intransitive()
    val liftMongoRecord = "net.liftweb"    %% "lift-mongodb-record" % "2.4-M5"

You can substitute "2.4-M2" for whatever version of Lift you are using. Rogue has been used in
production against Lift 2.2 and 2.4-M2. If you encounter problems using Rogue with other versions
of Lift, please let us know.

Join the [rogue-users google group](http://groups.google.com/group/rogue-users) for help, bug reports,
feature requests, and general discussion on Rogue.

## More Examples

[QueryTest.scala](https://github.com/foursquare/rogue/blob/master/src/test/scala/com/foursquare/rogue/QueryTest.scala) contains sample Records and examples of every kind of query supported by Rogue.
It also indicates what each query translates to in MongoDB's JSON query language.
It's a good place to look when getting started using Rogue.

NB: The examples in QueryTest only construct query objects; none are actually executed.
Once you have a query object, the following operations are supported (listed here because
they are not demonstrated in QueryTest):

For "find" query objects

    val query = Venue where (_.venuename eqs "Starbucks")
    query.count()
    query.countDistinct(_.mayor)
    query.fetch()
    query.fetch(n)
    query.get() // equivalent to query.fetch(1).headOption
    query.foreach{v: Venue => ... }
    query.paginate(pageSize)
    query.fetchBatch(pageSize){vs: List[Venue] => ...}
    query.bulkDelete_!!
    query.blockingBulkDelete_!!
    query.findAndDeleteOne()

For "modify" query objects

    val modify = query modify (_.popularity inc 1)
    modify.updateMulti()
    modify.updateOne()
    modify.upsertOne()

for "findAndModify" query objects

    val modify = query where (_.legacyid eqs 222) findAndModify (_.zip setTo "10003")
    modify.updateOne(returnNew = ...)
    modify.upsertOne(returnNew = ...)

## Releases

The latest release is 2.0.0-beta7. See the [changelog](https://github.com/foursquare/rogue/blob/master/CHANGELOG.md) for more details.

New since 1.1.0:

- iteratee support
- default WriteConcern is configurable
- renamed blockingBulkDelete_!! to bulkDelete_!! (takes a WriteConcern)
- moved gt, lt into base QueryField (nsanch)
- fixed the way nested subfield queries work if both fields are ListFields
- fixed handling of subfields of list fields
- allow nested subfields for BsonRecordFields
- removed EmptyQuery, fixed handling of upserts on empty queries
- BaseQuery.asDBObject, BaseModifyQuery.asDBObject
- fix for subselecting when the top-level field doesn't exist
- fixed bug where findAndModify upsert with returnNew=false was returning Some
- fixed bug where $regex query on a field would not allow other queries on that field
- bumped mongo java driver version to 2.7.3
- publishing to sonatype instead of scala-tools
- allow $or queries in modify commands
- select/selectCase up to 10 fields (davidtaylor)
- only validate lists on $all and $in queries (jliszka)
- pass query object to logging hook (jliszka)

New in 1.1.0:

- Compile-time index checking! (nsanch)
- QueryLogger.logIndexHit hook (jliszka)
- use distinct values in $in and $all queries (jliszka)
- slaveOk query modifier (nsanch)

Lots of new features since 1.0.18!:

- end-to-end tests (nsanch)
- subfield select on embedded list (nsanch)
- regex match operator for string fields (jliszka)
- support for the $ positional operator
- pullWhere - $pull by query instead of exact match
- Mongo index checking (see [here](https://github.com/foursquare/rogue/blob/master/docs/Indexing.md) for documentation)
- $rename support
- ability to supply a WriteConcern to updateOne, updateMulti and upsertOne
- select and selectCase can handle 7 and 8 parameters
- $bit support
- improved support for subfield queries on BsonRecordField
- added "matches" operator (for regexes) on StringFields with explicit index behavior expectations
- sbt 0.10.0
- raw access do BasicDBObjectBuilder in the query builder
- whereOpt support: Venue.whereOpt(uidOpt)(_.userid eqs _)
- findAndModify support
- $or query support
- efficient .exists query method (thanks Jorge!)
- support for BsonRecordField and BsonRecordListField (thanks Marc!)
- type-safe foreign key condtions, e.g., Tip.where(_.venueid eqs venueObj) (thanks dtaylor!)

Please see [QueryTest.scala](https://github.com/foursquare/rogue/blob/master/src/test/scala/com/foursquare/rogue/QueryTest.scala) for examples of these new features.

Other recent notable changes:

- .toString produces runnable javascript commands for mongodb console
- added tests for constructions that should not compile
- selectCase() builder method for select()ing via case class
- added blockingBulkDelete_!! method which takes a WriteConcern
- index hinting support
- support for selecting subfields
- support for $maxScan and $comment addSpecial parameters on find() queries
- explain() method on BaseQuery (thanks tjulien!)
- query signatures: string version of a query with all values blanked out
- support for indicating when a query clause is intended to hit an index (for runtime index checking, if you wish to implement it)


## Dependencies

lift-mongodb-record (2.2, 2.3, 2.4), mongodb, joda-time, junit. These dependencies are managed by the build system.

## Maintainers

Rogue was initially developed by Foursquare Labs for internal use -- 
nearly all of the MongoDB queries in foursquare's code base go through this library.
The current maintainers are:

- Jason Liszka jliszka@foursquare.com
- Jorge Ortiz jorge@foursquare.com
- Neil Sanchala nsanch@foursquare.com

Contributions welcome!