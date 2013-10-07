# Rogue

Rogue is a type-safe internal Scala DSL for constructing and executing find and modify commands against
MongoDB in the Lift web framework. It is fully expressive with respect to the basic options provided
by MongoDB's native query language, but in a type-safe manner, building on the record types specified in 
your Lift models. An example:

    Venue.where(_.mayor eqs 1234).and(_.tags contains "Thai").fetch(10)

The type system enforces the following constraints:

- the fields must actually belong to the record (e.g., mayor is a field on the Venue record)
- the field type must match the operand type (e.g., mayor is an IntField)
- the operator must make sense for the field type (e.g., categories is a MongoListField[String])

In addition, the type system ensures that certain builder methods are only used in certain circumstances.
For example, take this more complex query:

    Venue.where(_.closed eqs false).orderAsc(_.popularity).limit(10).modify(_.closed setTo true).updateMulti

This query purportedly finds the 10 least popular open venues and closes them. However, MongoDB
does not (currently) allow you to specify limits on modify queries, so Rogue won't let you either.
The above will generate a compiler error. 

Constructions like this:

    def myMayorships = Venue.where(_.mayor eqs 1234).limit(5)
    ...
    myMayorships.fetch(10)

will also not compile, here because a limit is being specified twice. Other similar constraints
are in place to prevent you from accidentally doing things you don't want to do anyway.

## Installation

Because Rogue is designed to work with several versions of lift-mongodb-record (2.2, 2.3, 2.4),
you'll want to declare your dependency on Rogue as `intransitive` and declare an explicit dependency
on the version of Lift you want to target. In sbt, that would look like the following: 

    val rogueField      = "com.foursquare" %% "rogue-field"         % "2.2.0" intransitive()
    val rogueCore       = "com.foursquare" %% "rogue-core"          % "2.2.0" intransitive()
    val rogueLift       = "com.foursquare" %% "rogue-lift"          % "2.2.0" intransitive()
    val rogueIndex      = "com.foursquare" %% "rogue-index"         % "2.2.0" intransitive()
    val liftMongoRecord = "net.liftweb"    %% "lift-mongodb-record" % "2.4"

You can substitute "2.5.1" for whatever version of Lift you are using. Rogue has been used in
production against Lift 2.4 and 2.5.1. If you encounter problems using Rogue with other versions
of Lift, please let us know.

Join the [rogue-users google group](http://groups.google.com/group/rogue-users) for help, bug reports,
feature requests, and general discussion on Rogue.

## Setup

Define your record classes in Lift like you would normally (see [TestModels.scala](https://github.com/foursquare/rogue/blob/master/rogue-lift/src/test/scala/com/foursquare/rogue/TestModels.scala) for examples).

Then anywhere you want to use rogue queries against these records, import the following:

    import com.foursquare.rogue.LiftRogue._

See [EndToEndTest.scala](https://github.com/foursquare/rogue/blob/master/rogue-lift/src/test/scala/com/foursquare/rogue/EndToEndTest.scala) for a complete working example.

## More Examples

[QueryTest.scala](https://github.com/foursquare/rogue/blob/master/rogue-lift/src/test/scala/com/foursquare/rogue/QueryTest.scala) contains sample Records and examples of every kind of query supported by Rogue.
It also indicates what each query translates to in MongoDB's JSON query language.
It's a good place to look when getting started using Rogue.

NB: The examples in QueryTest only construct query objects; none are actually executed.
Once you have a query object, the following operations are supported (listed here because
they are not demonstrated in QueryTest):

For "find" query objects

    val query = Venue.where(_.venuename eqs "Starbucks")
    query.count()
    query.countDistinct(_.mayor)
    query.fetch()
    query.fetch(n)
    query.get()     // equivalent to query.fetch(1).headOption
    query.exists()  // equivalent to query.fetch(1).size > 0
    query.foreach{v: Venue => ... }
    query.paginate(pageSize)
    query.fetchBatch(pageSize){vs: List[Venue] => ...}
    query.bulkDelete_!!(WriteConcern.SAFE)
    query.findAndDeleteOne()
    query.explain()
    query.iterate(handler)
    query.iterateBatch(batchSize, handler)

For "modify" query objects

    val modify = query.modify(_.mayor_count inc 1)
    modify.updateMulti()
    modify.updateOne()
    modify.upsertOne()

for "findAndModify" query objects

    val modify = query.where(_.legacyid eqs 222).findAndModify(_.closed setTo true)
    modify.updateOne(returnNew = ...)
    modify.upsertOne(returnNew = ...)

## Releases

The latest release is 2.2.0. See the [changelog](https://github.com/foursquare/rogue/blob/master/CHANGELOG.md) for more details.

Major changes in 2.0.0:

- separated the query execution logic from the query builder
- removed dependency on Lift for ORM, although Lift is still the default

## Dependencies

[rogue-field](https://github.com/foursquare/rogue-field), lift-mongodb-record (2.2, 2.3, 2.4), mongodb, joda-time, junit. These dependencies are managed by the build system.

## Maintainers

Rogue was initially developed by Foursquare Labs for internal use -- 
nearly all of the MongoDB queries in foursquare's code base go through this library.
The current maintainers are:

- Jason Liszka jliszka@foursquare.com
- Jorge Ortiz jorge@foursquare.com
- Neil Sanchala nsanch@foursquare.com

Contributions welcome!
