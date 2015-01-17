libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val liftVersion = scalaVersion match {
    case "2.11.5"          => "2.6-RC1"
    case "2.10.4"          => "2.6-RC1"
  }
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.11.5" => "2.11"
      case "2.10.4" => "2.10"
  })
  Seq(
    "net.liftweb"              % sv("lift-util")           % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-common")         % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-record")         % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-mongodb-record") % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-mongodb")        % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-webkit")         % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-json")           % liftVersion  % "compile",
    "joda-time"                % "joda-time"               % "2.1"        % "compile",
    "org.joda"                 % "joda-convert"            % "1.2"        % "compile",
    "org.mongodb"              % "mongo-java-driver"       % "2.11.3"     % "compile")
}

Seq(RogueBuild.defaultSettings: _*)
