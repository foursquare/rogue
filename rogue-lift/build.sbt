libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val liftVersion = scalaVersion match {
    case "2.11.2"          => "2.6-M4"
    case "2.10.2"          => "2.5.1"
    case "2.9.1" | "2.9.2" => "2.4"
    case _                 => "2.4-M2"
  }
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.11.2" => "2.11"
      case "2.10.2" => "2.10"
      case "2.9.2"  => "2.9.1"
      case v => v
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
