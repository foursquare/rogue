libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val spindleVersion = "1.8.4"
  Seq(
    "com.foursquare"          %  "common-thrift-bson"   % spindleVersion,
    "org.mongodb"             %  "mongo-java-driver"    % "2.12.1"      % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)

Seq(thriftSettings: _*)
