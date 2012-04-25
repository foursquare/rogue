libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val liftVersion = scalaVersion match {
    case "2.9.1" => "2.4-M5"
    case _       => "2.4-M2"
  }
  Seq(
    "net.liftweb"             %% "lift-util"           % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-common"         % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-record"         % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-mongodb-record" % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-mongodb"        % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-webkit"         % liftVersion  % "compile" intransitive(),
    "net.liftweb"             %% "lift-json"           % liftVersion  % "compile",
    "joda-time"                % "joda-time"           % "2.1"        % "compile",
    "org.joda"                 % "joda-convert"        % "1.2"        % "compile")
}

Seq(RogueBuild.defaultSettings: _*)
