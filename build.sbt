name := "rogue"

version := "1.1.5-SNAPSHOT"

organization := "com.foursquare"

crossScalaVersions := Seq("2.9.1", "2.9.0-1", "2.9.0", "2.8.1", "2.8.0")

libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val specsVersion = scalaVersion match {
    case "2.8.0" => "1.6.5"
    case "2.9.1" => "1.6.9"
    case _       => "1.6.8"
  }
  val liftVersion = scalaVersion match {
    case "2.9.1" => "2.4-M5"
    case _       => "2.4-M2"
  }
  Seq(
    "net.liftweb"             %% "lift-mongodb-record" % liftVersion  % "compile" excludeAll(
       ExclusionRule(organization = "org.mongodb")
    ),
    "org.mongodb"              % "mongo-java-driver"   % "2.7.3"      % "compile",
    "junit"                    % "junit"               % "4.5"        % "test",
    "com.novocode"             % "junit-interface"     % "0.6"        % "test",
    "ch.qos.logback"           % "logback-classic"     % "0.9.26"     % "provided",
    "org.scala-tools.testing" %% "specs"               % specsVersion % "test",
    "org.scala-lang"           % "scala-compiler"      % scalaVersion % "test"
  )
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

publishTo <<= (version) { v =>
  val nexus = "http://oss.sonatype.org/"
  if (v.endsWith("-SNAPSHOT"))
    Some("snapshots" at nexus+"content/repositories/snapshots")
  else
    Some("releases" at nexus+"service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>http://github.com/foursquare/rogue</url>
  <licenses>
    <license>
      <name>Apache</name>
      <url>http://www.opensource.org/licenses/Apache-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:foursquare/rogue.git</url>
    <connection>scm:git:git@github.com:foursquare/rogue.git</connection>
  </scm>
  <developers>
    <developer>
      <id>jliszka</id>
      <name>Jason Liszka</name>
      <url>http://github.com/jliszka</url>
    </developer>
  </developers>)

resolvers += "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/"

resolvers <++= (version) { v =>
  if (v.endsWith("-SNAPSHOT"))
    Seq(ScalaToolsSnapshots)
  else
    Seq()
}

scalacOptions ++= Seq("-deprecation", "-unchecked")

testFrameworks += new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker")


credentials ++= {
  val sonatype = ("Sonatype Nexus Repository Manager", "oss.sonatype.org")
  def loadMavenCredentials(file: java.io.File) : Seq[Credentials] = {
    xml.XML.loadFile(file) \ "servers" \ "server" map (s => {
      val host = (s \ "id").text
      val realm = if (host == sonatype._2) sonatype._1 else "Unknown"
      Credentials(realm, host, (s \ "username").text, (s \ "password").text)
    })
  }
  val ivyCredentials   = Path.userHome / ".ivy2" / ".credentials"
  val mavenCredentials = Path.userHome / ".m2"   / "settings.xml"
  (ivyCredentials.asFile, mavenCredentials.asFile) match {
    case (ivy, _) if ivy.canRead => Credentials(ivy) :: Nil
    case (_, mvn) if mvn.canRead => loadMavenCredentials(mvn)
    case _ => Nil
  }
}
