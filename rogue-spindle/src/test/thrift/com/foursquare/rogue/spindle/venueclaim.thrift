namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"
include "com/foursquare/rogue/spindle/venue.thrift"

typedef types.ThriftMetadata ThriftMetadata

enum ThriftRejectReason {
  tooManyClaims = 0 (string_value="too many claims")
  cheater = 1 (string_value="cheater")
  wrongCode = 2 (string_value="wrong code")
}

struct ThriftVenueClaim {
  1: optional ThriftMetadata metadata
  2: optional ids.VenueClaimId id (wire_name="_id")
  3: optional ids.VenueId venueId (wire_name="vid")
  4: optional ids.UserId userId (wire_name="uid")
  5: optional venue.ThriftClaimStatus status (wire_name="status")
  6: optional ThriftRejectReason reason (wire_name="reason")
} (
  primary_key="id"
  foreign_key="venueId"
  foreign_key="userId"
  mongo_identifier="rogue_mongo"
  mongo_collection="venueclaims"
)
