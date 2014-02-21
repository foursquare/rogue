namespace java com.foursquare.rogue.spindle.gen

// A place to store freeform metadata about an entity.
typedef map<string, string> ThriftMetadata

// The id of an entity. Always exactly 12 bytes.
typedef binary (enhanced_types="bson:ObjectId") ObjectId

// A UTC datetime, stored as millis since the epoch.
typedef i64 (enhanced_types="bson:DateTime") DateTime

// Number of days since epoch. 16 bits gets us to the year 2149.
typedef i16 ThriftLocalDate

// An S2 CellId.
typedef i64 ThriftS2CellId

// A pair of double coordinates.
typedef list<double> ThriftLatLng

// A lat/long struct
struct ThriftLatLngPair {
  1: optional double lat (wire_name="lat")
  2: optional double lng (wire_name="lng")
}

// A geographic bounding box specified by its NE and SW corners. It can optionally contain a Geometry object
struct ThriftBoundingBox {
  1: optional ThriftLatLngPair northEast
  2: optional ThriftLatLngPair southWest
  3: optional binary wkbGeometry
  4: optional double areaThreshold
  5: optional i64 geoId
}

// A geographic bounding box specified by its NE and SW corners
// with shorter names
struct ThriftLatLngRect {
  1: optional ThriftLatLngPair northEast (wire_name="ne")
  2: optional ThriftLatLngPair southWest (wire_name="sw")
}

// A geographic bounding circle specified by its center and radius
struct ThriftBoundingCircle {
  1: optional ThriftLatLngPair center
  2: optional double radius
}

// A geographic bounding S2 cover specified by its centroid and a list of S2 cells
struct ThriftS2CoverBound {
  1: optional ThriftLatLngPair centroid
  2: optional list<i64> s2CellIds
}

// A union that encodes one of three ways of specifying a geographic region
struct ThriftGeoBounds {
  1: optional ThriftBoundingBox box
  2: optional ThriftBoundingCircle circle
  3: optional ThriftS2CoverBound s2CoverBound
}

// The id of a User record.
typedef i64 ThriftUserId

// The id of a chain, which is a User record.
typedef ThriftUserId ThriftChainId

// A container that opaquely holds thrift structs. Do not use directly.
// See com.foursquare.common.types.MessageSet for docs.
struct MessageSetStruct {
  // The value is a MessageSetData serialized with TCompactProtocol.
  2: optional binary unparsed
} (
  enhanced_types="fs:MessageSet"
)

// A container used in the serialization/deserialization of MessageSet.
struct MessageSetData {
  1: optional map<i64, binary> data
}
