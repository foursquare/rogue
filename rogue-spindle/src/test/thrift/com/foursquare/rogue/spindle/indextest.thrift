namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"

typedef types.ThriftMetadata ThriftMetadata
typedef types.ObjectId ObjectId
typedef types.ThriftLatLng ThriftLatLng

// Model used for testing in IndexCheckerTest.scala

struct ThriftIndexTestModel {
  1: optional ThriftMetadata metadata
  2: optional ids.IndexTestId id (wire_name="_id")
  3: optional i32 a (wire_name="a")
  4: optional i32 b (wire_name="b")
  5: optional i32 c (wire_name="c")
  6: optional i32 d (wire_name="d")
  7: optional map<string, i32> m (wire_name="m")
  8: optional map<string, i32> n (wire_name="n")
  9: optional ThriftLatLng ll (wire_name="ll")
  10: optional list<i32> l (wire_name="l")
} (
   primary_key="id"
   index="id:asc"
   index="a:asc, b:asc, c:asc"
   index="m:asc, a:asc"
   index="l:asc"
   index="ll:2d, b:asc"
   mongo_identifier="rogue_mongo"
   mongo_collection="indextestmodel"
)
