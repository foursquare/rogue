libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val specsVersion = scalaVersion match {
    case "2.11.2" => "2.4.2"
    case "2.9.1" | "2.9.2" | "2.10.2" => "1.12.3"
    case _       => "1.5"
  }
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
    "net.liftweb"              % sv("lift-mongodb")    % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-common")     % liftVersion  % "compile",
    "net.liftweb"              % sv("lift-json")       % liftVersion  % "compile",
    "net.liftweb"              % sv("lift-util")       % liftVersion  % "compile",
    "joda-time"                % "joda-time"           % "2.1"        % "provided",
    "org.joda"                 % "joda-convert"        % "1.2"        % "provided",
    "org.mongodb"              % "mongo-java-driver"   % "2.11.3"     % "compile",
    "junit"                    % "junit"               % "4.5"        % "test",
    "com.novocode"             % "junit-interface"     % "0.6"        % "test",
    "ch.qos.logback"           % "logback-classic"     % "0.9.26"     % "provided",
    "org.specs2"              %% "specs2"              % specsVersion % "test",
    "org.scala-lang"           % "scala-compiler"      % scalaVersion % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)
