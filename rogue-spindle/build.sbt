libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val spindleVersion = "3.0.0-M1.2"
  Seq(
    "com.foursquare"          %  "common-thrift-bson"   % spindleVersion
  )
}

Seq(RogueBuild.defaultSettings: _*)

Seq(thriftSettings: _*)
