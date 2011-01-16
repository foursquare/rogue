# Rogue

Rogue is a type-safe internal DSL for querying and modifying MongoDB records in the Lift web framework.
It is fully expressive with respect to the basic options provided by MongoDB's native query language,
but in a type-safe manner, building on the record types specified in your Lift models. An example:

    Venue where (_.mayor eqs 1234) and (_.categories contains "Thai") fetch(10)

The type system enforces the following constraints:
- the fields must actually belong to the record (e.g., mayor is a field on the Venue record)
- the field type must match the operand type (e.g., mayor is an IntField)
- the operator must make sense for the field type (e.g., categories is a MongoListField)

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

will also not compile, here because a limit is being specified twice.

## More Examples

QueryTest.scala contains examples of every kind of query supported by Rogue, and also
indicates what each query translates to in MongoDB's JSON query language.

NB: The examples in QueryTest only construct query objects; none are actually executed.
Once you have a query object, the following operations are supported (listed here because
they are not demonstrated in QueryTest):

For "find" query objects

    val query = Venue where (_.venuename eqs "Starbucks")
    query.count()
    query.countDistinct(_.mayor)
    query.fetch()
    query.fetch(10)
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

liftweb-2.2, mongodb, joda-time, junit. These dependencies are managed by the build system.

## Building

Use sbt (simple-build-tool) to build:

    $ sbt clean update package

The finished jar will be in `target/`.

## Maintainers

Jason Liszka jliszka@foursquare.com
Jorge Ortiz jorge@foursquare.com

