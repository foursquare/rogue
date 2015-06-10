libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val spindleVersion = "3.0.0-M13"
  Seq(
    "com.foursquare"          %  "common-thrift-bson"   % spindleVersion,
    "org.mongodb"             %  "mongo-java-driver"    % "2.13.2"      % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)

Seq(thriftSettings: _*)
