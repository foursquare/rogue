namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"

typedef types.ThriftMetadata ThriftMetadata
typedef types.ObjectId ObjectId

struct ThriftOneCommentBson {
  1: optional string timestamp (wire_name="timestamp")
  2: optional i64 userid (wire_name="userid")
  3: optional string comment (wire_name="comment")
}

struct ThriftComment {
  1: optional ThriftMetadata metadata
  2: optional ids.CommentId id (wire_name="_id")
  3: optional list<ThriftOneCommentBson> comments (wire_name="comments")
} (
  primary_key="id"
  index="id:asc"
  mongo_identifier="rogue_mongo"
  mongo_collection="comments"
)
