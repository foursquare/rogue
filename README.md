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

## Building and Installation

Use sbt (simple-build-tool) to build:

    $ sbt clean update package

The finished jar will be in `target/`.

Or if you prefer, packaged JARs are available on scala-tools.org. Add the following line to your
sbt project file to pull them in:

    val rogue = "com.foursquare" %% "rogue" % "1.0.5" withSources()

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

For "modify" query objects

    val modify = query modify (_.popularity inc 1)
    modify.updateMulti()
    modify.updateOne()
    modify.upsertOne()

## Dependencies

liftweb (2.2, 2.3), mongodb, joda-time, junit. These dependencies are managed by the build system.

## Maintainers

Rogue was initially developed by Foursquare Labs for internal use -- 
nearly all of the MongoDB queries in foursquare's code base go through this library.
The current maintainers are:

- Jason Liszka jliszka@foursquare.com
- Jorge Ortiz jorge@foursquare.com

Contributions welcome!