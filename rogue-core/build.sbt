libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val specsVersion = scalaVersion match {
    case "2.9.2" => "1.12.3"
    case "2.9.1" => "1.12.3"
    case "2.9.0" => "1.6.1"
    case _       => "1.5"
  }
  val liftVersion = scalaVersion match {
    case "2.9.1" | "2.9.2" => "2.4-M5"
    case _                 => "2.4-M2"
  }
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.9.2" => "2.9.1"
      case v => v
  })
  Seq(
    "net.liftweb"              % sv("lift-mongodb")    % liftVersion  % "compile" intransitive(),
    "net.liftweb"              % sv("lift-common")     % liftVersion  % "compile",
    "net.liftweb"              % sv("lift-json")       % liftVersion  % "compile",
    "joda-time"                % "joda-time"           % "2.1"        % "provided",
    "org.joda"                 % "joda-convert"        % "1.2"        % "provided",
    "org.mongodb"              % "mongo-java-driver"   % "2.9.3"      % "compile",
    "junit"                    % "junit"               % "4.5"        % "test",
    "com.novocode"             % "junit-interface"     % "0.6"        % "test",
    "ch.qos.logback"           % "logback-classic"     % "0.9.26"     % "provided",
    "org.specs2"              %% "specs2"              % specsVersion % "test",
    "org.scala-lang"           % "scala-compiler"      % scalaVersion % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)
