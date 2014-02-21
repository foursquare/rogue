namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"

typedef types.ThriftMetadata ThriftMetadata
typedef types.ObjectId ObjectId

enum ThriftConsumerPrivilege {
  awardBadges = 0 (string_value="Award badges")
}

struct ThriftOAuthConsumer {
  1: optional ThriftMetadata metadata
  2: optional ids.OAuthConsumerId id (wire_name="_id")
  3: optional list<ThriftConsumerPrivilege> privileges (wire_name="privileges")
} (
  primary_key="id"
  mongo_identifier="rogue_mongo"
  mongo_collection="oauthconsumers"
)
