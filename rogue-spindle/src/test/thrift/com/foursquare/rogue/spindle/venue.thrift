namespace java com.foursquare.rogue.spindle.gen

include "com/foursquare/rogue/spindle/ids.thrift"
include "com/foursquare/rogue/spindle/types.thrift"

typedef types.ThriftMetadata ThriftMetadata
typedef types.ObjectId ObjectId
typedef types.ThriftLatLng ThriftLatLng
typedef types.DateTime DateTime

enum ThriftVenueStatus {
  open = 0 (string_value="Open")
  closed = 1 (string_value="Closed")
}

enum ThriftClaimStatus {
  pending = 0 (string_value="Pending approval")
  approved = 1 (string_value="Approved")
}

struct ThriftSourceBson {
  1: optional string name (wire_name="name")
  2: optional string url (wire_name="url")
}

struct ThriftVenueClaimBson {
  1: optional i64 userid (wire_name="uid")
  2: optional ThriftClaimStatus status (wire_name="status")
  3: optional ThriftSourceBson source (wire_name="source")
}

struct ThriftCheckin {
  1: optional ObjectId id (wire_name="_id")
  2: optional DateTime date
}

struct ThriftVenue {
  1: optional ThriftMetadata metadata
  2: optional ids.VenueId id (wire_name="_id")
  3: optional i64 legacyid (wire_name="legid")
  4: optional i64 userid (wire_name="userid")
  5: optional string venuename (wire_name="venuename")
  6: optional i64 mayor (wire_name="mayor")
  7: optional i32 mayor_count (wire_name="mayor_count")
  8: optional bool closed (wire_name="closed")
  9: optional list<string> tags (wire_name="tags")
  10: optional list<i32> popularity (wire_name="popularity")
  11: optional list<ObjectId> categories (wire_name="categories")
  12: optional ThriftLatLng geolatlng (wire_name="latlng")
  13: optional DateTime last_updated (wire_name="last_updated")
  14: optional ThriftVenueStatus status (wire_name="status")
  15: optional list<ThriftVenueClaimBson> claims (wire_name="claims")
  16: optional ThriftVenueClaimBson lastClaim (wire_name="last_claim")
  17: optional map<string, ThriftCheckin> lastCheckins
} (
   primary_key="id"
   index="id:asc"
   index="mayor:asc, id:asc"
   index="mayor:asc, id:asc, closed:asc"
   index="legacyid:asc"
   index="geolatlng:2d"
   index="geolatlng:2d, tags:asc"
   mongo_identifier="rogue_mongo"
   mongo_collection="venues"
)
