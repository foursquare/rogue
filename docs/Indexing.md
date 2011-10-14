# Index Checking using the Rogue IndexChecker

To use index checking, you need to be able to do two things:

1. You need to provide information about what indexes you have in Mongo for your collections.
2. You need to be able to find out what indexes are expected by your query.

In Rogue, our query objects automatically compute the indices that are expected by a given query, so as long as you use Rogue queries, you're in good shape. All you need to do is provide information about your own datatypes.


## Providing Indexes for `MongoRecord` types

The way to provide indexing information is to use the trait `com.foursquare.rogue.IndexedRecord`. `IndexedRecord` contains one field, `val mongoIndexList: List[MongoIndex[_]]`. If your record type inherits from `IndexedRecord`, then Rogue will automatically retrieve the index from the record type.

For example, consider a simple record type:

	> class SampleModel extends MongoRecord[SampleModel] with MongoId[SampleModel] 
	>{
	>  def meta = SampleModel
	>  object a extends IntField(this)	
	>  object b extends StringField(this)
	>}
	>
	>object SampleModel extends SampleModel with MongoMetaRecord[SampleModel] with IndexedRecord[TestModel] {
	>  override def collectionName = "model"
	>  override val mongoIndexList = List(
	>    SampleModel.index(_._id, Asc),
	>    SampleModel.index(_.a, Asc, _.b, Desc))
	>}

The indexes are provided by overriding `val mongoIndexList`, and providing a list of index objects for the indexes that you will have in your mongo database. For simple field expressions, you can use `.index(_.fieldname, dir)`, where "dir" is the index type. For simple types like this, that's basically the sorting direction - `Asc` for ascending, or `Desc` for descending. There are a collection of index-types that are provided by Rogue, which you can find in the source file `Rogue.scala`. You can also add your own, by subclassing the case-class `IndexModifier`.

You can index more complex field expressions. Basically, if you can write a Mongo field expression in Scala, you can use that field expression for an index. For example, in foursquare code, we have a type that contains a Mongo map field. We can describe an index that uses the map keys: `Type.index(_.mapField.unsafeField[String]("key")`.


## Checking Indexes for a Query

To check the indexes for a query, there are two different index checking methods that you can call:

1. `MongoIndexChecker.validateIndexExpectations(query)`: this retrieves the list of indexes for the type being queried, and verifies that all of the fields that your query expects to be indexed are, in fact, indexed.
2. `MongoIndexChecker.validateQueryMatchesSomeIndex(query)`: this retrieves the list of indexes, and confirms not only that the indexes exist, but that Mongo will successfully find the correct indexes to perform the query. (There are conditions where there is an index that could, theoretically, be used by a query, but where Mongo will not recognize that the index is usable.)
