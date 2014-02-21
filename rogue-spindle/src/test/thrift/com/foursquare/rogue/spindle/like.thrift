namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"

typedef types.ThriftMetadata ThriftMetadata
typedef types.ObjectId ObjectId

struct ThriftLike {
  1: optional ThriftMetadata metadata
  2: optional ids.LikeId id (wire_name="_id")
  3: optional i64 userid (wire_name="userid")
  4: optional i64 checkin (wire_name="checkin")
  5: optional ObjectId tip (wire_name="tip")
} (
  primary_key="id"
  mongo_identifier="rogue_mongo"
  mongo_collection="likes"
)
