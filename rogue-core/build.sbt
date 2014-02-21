libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val specsVersion = scalaVersion match {
    case "2.9.1" | "2.9.2" | "2.10.2" => "1.12.3"
    case "2.9.0" => "1.6.1"
    case _       => "1.5"
  }
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.10.2" => "2.10"
      case "2.9.2"  => "2.9.1"
      case v => v
  })
  Seq(
    "com.foursquare"           % sv("rogue-field")     % "2.2.1"      % "compile",
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
