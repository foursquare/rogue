libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val spindleVersion = "1.7.1"
  Seq(
    "com.foursquare"          %  "common-thrift-bson"   % spindleVersion,
    "org.mongodb"             %  "mongo-java-driver"    % "2.11.3"      % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)

Seq(thriftSettings: _*)
