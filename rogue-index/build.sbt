libraryDependencies <++= (scalaVersion) { scalaVersion =>
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.10.3" => "2.10"
      case "2.9.2"  => "2.9.1"
      case v => v
  })
  Seq(
    "com.foursquare"           % sv("rogue-field")     % "2.2.1"      % "compile"
  )
}

Seq(RogueBuild.defaultSettings: _*)
