libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val spindleVersion = "3.0.0-M4.2"
  Seq(
    "com.foursquare"          %  "common-thrift-bson"   % spindleVersion,
    "org.mongodb"             %  "mongo-java-driver"    % "2.11.3"      % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)

Seq(thriftSettings: _*)
