libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val spindleVersion = "1.7.0"
  Seq(
    "com.foursquare"          %  "common-thrift-bson"   % spindleVersion
  )
}

Seq(RogueBuild.defaultSettings: _*)

Seq(thriftSettings: _*)
