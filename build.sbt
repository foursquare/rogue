name := "rogue"

version := "1.0.14-SNAPSHOT"

organization := "com.foursquare"

scalaVersion := "2.8.1"

//seq(WebPlugin.webSettings :_*)

libraryDependencies ++= {
  val liftVersion = "2.4-M2"
  Seq(
    "net.liftweb" %% "lift-mongodb-record" % liftVersion % "compile->default",
    "junit" % "junit" % "4.5" % "test->default",
    "com.novocode" % "junit-interface" % "0.6" % "test",
    "ch.qos.logback" % "logback-classic" % "0.9.26",
    "org.scala-tools.testing" %% "specs" % "1.6.8" % "test->default"
  )
}

publishTo <<= (version) { version: String =>
  val nexus = "http://nexus-direct.scala-tools.org/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/") 
  else                                   Some("releases" at nexus+"releases/")
}

resolvers += "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/"

resolvers += ScalaToolsSnapshots

scalacOptions += "-deprecation"

javacOptions ++= Seq("-Xmx4096m", "-Xms512m", "-Xss4m")

testFrameworks += new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker")

defaultExcludes ~= (_ || "*~")

credentials ++= {
  val scalaTools = ("Sonatype Nexus Repository Manager", "nexus.scala-tools.org")
  def loadMavenCredentials(file: java.io.File) : Seq[Credentials] = {
    xml.XML.loadFile(file) \ "servers" \ "server" map (s => {
      val host = (s \ "id").text
      val realm = if (host == scalaTools._2) scalaTools._1 else "Unknown"
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

publishMavenStyle := true
