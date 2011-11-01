# Index Checking using the Rogue IndexChecker

To use index checking, you need to do 3 things:

1. Provide information about what indexes you have in Mongo for your collections.
2. Annotate your queries to indicate how you expect them to use indexes.
3. Call the rogue index checker methods from the QueryValidator callbacks.

The index checker methods validate the index behavior expectations annotated in each query against the actual indexes present on the collection by simulating the logic of the Mongo query planner. If the index expectations do not match what Rogue thinks the Mongo query planner will do, you will get a warning (using whatever logging hooks you have set up).

## Providing Indexes in MongoRecords

The way to provide indexing information is to use the trait `com.foursquare.rogue.IndexedRecord`. `IndexedRecord` contains one field, `val mongoIndexList: List[MongoIndex[_]]`. If your record type inherits from `IndexedRecord`, then Rogue will automatically retrieve the index from the record type.

For example, consider a simple record type:

```class SampleModel extends MongoRecord[SampleModel] with MongoId[SampleModel] {
  def meta = SampleModel
  object a extends IntField(this)	
  object b extends StringField(this)
}
```
```
object SampleModel extends SampleModel with MongoMetaRecord[SampleModel] with IndexedRecord[TestModel] {
  override def collectionName = "model"
  override val mongoIndexList = List(
    SampleModel.index(_._id, Asc),
    SampleModel.index(_.a, Asc, _.b, Desc))
}
```

The indexes are provided by overriding `val mongoIndexList`, and providing a list of index objects for the indexes that you will have in your mongo database. For simple field expressions, you can use `.index(_.fieldname, dir)`, where "dir" is the index type. For simple types like this, that's basically the sorting direction - `Asc` for ascending, or `Desc` for descending. There are a collection of index-types that are provided by Rogue, which you can find in the source file `Rogue.scala`. You can also add your own, by subclassing the case-class `IndexModifier`.

You can index more complex field expressions. Basically, if you can write a Mongo field expression in Scala, you can use that field expression for an index. For example, in foursquare code, we have a type that contains a Mongo map field. We can describe an index that uses the map keys: `Type.index(_.mapField.unsafeField[String]("key")`.

## Annotating Queries

Of course, not every query 100% matches an index! For instance, here's an index declared on Campaign:

```{ groupids: 1, active: 1 }
```

And say you have this query:

```Campaign.where(_.groupids in allGroups.map(_.id))
        .and(_.active eqs true)
        .and(_.userid eqs user.id.is)
        .fetch()
```

The query clearly matches that index! But how does the index checker know to ignore the userid part of the query? It doesn't! Unless you do this:

```Campaign.where(_.groupids in allGroups.map(_.id))
        .and(_.active eqs true)
        .scan(_.userid eqs user.id.is)
        .fetch()
```

If you say `scan` instead of `where` or `and`, the index checker will ignore that clause and try to match the rest of the clauses exactly to an index. It is also a visual indication to the reader which part of the query is hitting an index and which part will result in a scan.

**NB**: Using `scan` does not _cause_ a scan, it just tells the index checker that you expect that clause to result in a scan.  The `where`, `and` and `scan` methods are all functionally equivalent outside of the index checker.

In addition to the document scan, there is also the notion of an index scan. An index scan can happen when the query can be answered by the index, but a large number of index entries must be scanned in order to find the relevant results. There are a few ways this can happen, which you'll see below. Use `iscan` to indicate to the index checker that you expect an index scan to happen.

The examples below assume a collection of `Thing`s with fields `a`, `b`, `c` and `d` and an index `{a:1, b:1, c:1}`.

**Document scan**: the records themselves must be scanned in order to figure out which ones satisfy the query. Use `scan`.

- An operator is used that cannot be answered by the index. These are `$exists, $nin and $size`.
  ```
  Thing scan (_.a exists false)```
- A query field does not appear in the index.
  ```
  Thing where (_.a eqs 1) scan (_.d eqs 4)```
- The first field in the index does not appear in the query. (compare "a level is skipped" below)
  ```
  Thing scan (_.b eqs 2)```

**Index scan**: The query can be answered by the index, but multiple (possibly non-matching) index entries need to be examined. Use `iscan`.

- An operator is used that requires an index scan. These are `$mod` and `$type`.
  ```
  Thing iscan (_.a hastype MongoType.String)```
- An operator is used that requires a partial index scan. These are the "range" operators `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$near` and `$within`.
```
  Thing iscan (_.a > 1)
  Thing iscan (_.a > 1) iscan (_.b > 2)
  Thing where (_.a eqs 1) iscan (_.b > 2)
  Thing iscan (_.b > 2) and (_.a eqs 1) // NB: equivalent to previous
```
- A clause that would otherwise hit an index follows (in the index, not in the query) a clause that causes a partial index scan.
  ```
  Thing iscan (_.a > 1) iscan (_.b eqs 2)```
- A level is skipped in the index.
  ```
  Thing where (_.a eqs 1) iscan (_.c eqs 3)```

## Checking Indexes

To check the indexes for a query, there are two different index checking methods that you can call:

1. `MongoIndexChecker.validateIndexExpectations(query)`: this retrieves the list of indexes for the type being queried, and verifies that all of the fields that your query expects to be indexed are, in fact, indexed.
2. `MongoIndexChecker.validateQueryMatchesSomeIndex(query)`: this retrieves the list of indexes, and confirms not only that the indexes exist, but that Mongo will successfully find the correct indexes to perform the query. (There are conditions where there is an index that could, theoretically, be used by a query, but where Mongo will not recognize that the index is usable.)

You can call either or both of these from the `validateQuery` and/or `validateModify` callbacks in the `QueryValidator` hook. For example,
when your application boots, set up the hooks like so:

```object MyQueryValidator extends QueryHelpers.DefaultQueryValidator {
  override def validateQuery[M <: MongoRecord[M]](query: Implicits.GenericBaseQuery[M, _]) {
  if (Props.mode != Props.RunModes.Production) {
    MongoIndexChecker.validateIndexExpectations(query) &&
    MongoIndexChecker.validateQueryMatchesSomeIndex(query)
  }
```
```
  override def validateModify[M <: MongoRecord[M]](modify: BaseModifyQuery[M]) {
    validateQuery(modify.query)
  }
```
```
  override def validateFindAndModify[M <: MongoRecord[M], R](modify: BaseFindAndModifyQuery[M, R]) {
    validateQuery(modify.query)
  }
}
```
```
QueryHelpers.validator = MyQueryValidator
```

When the index checker has something to warn about, it will call the `QueryHelpers.QueryLogger.logIndexMismatch` callback.
At foursquare we have implemented it like so:

```override def logIndexMismatch(msg: => String) {
  val stack = Utils.currentStackTrace()
  val prefix = stack.indexOf("validateQuery(MongoSetup.scala") // a hack
  val trimmedStack = stack.drop(prefix).take(800)
  LOG.warn(msg + " from " + trimmedStack)
  if (services.exists(_.throttleService.throwOnQueryIndexMiss.isShown)) {
    throw new Exception(msg)
  }
}
```

