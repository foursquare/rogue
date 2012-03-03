libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val liftVersion = scalaVersion match {
    case "2.9.1" => "2.4-M4"
    case _       => "2.4-M2"
  }
  Seq(
    "net.liftweb"             %% "lift-mongodb-record" % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-record"         % liftVersion  % "compile")
}

Seq(defaultSettings: _*)
