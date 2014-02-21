namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"

typedef types.ThriftMetadata ThriftMetadata
typedef types.ObjectId ObjectId

typedef map<string, i32> ThriftTipCountMap

struct ThriftTip {
  1: optional ThriftMetadata metadata
  2: optional ids.TipId id (wire_name="_id")
  3: optional i64 legacyid (wire_name="legid")
  4: optional ThriftTipCountMap counts (wire_name="counts")
  5: optional i64 userid (wire_name="userid")
} (
  primary_key="id"
  index="id:asc"
  mongo_identifier="rogue_mongo"
  mongo_collection="tips"
)

struct ThriftTips {
  1: optional list<ThriftTip> tips (wire_name="tips")
}
