import sbt._

class RogueProject(info: ProjectInfo) extends DefaultProject(info) with Credential {
  val liftVersion = property[Version]
  // override def compileOptions = super.compileOptions ++ Seq(Unchecked)
  override def packageSrcJar = defaultJarPath("-sources.jar")
  val sourceArtifact = Artifact.sources(artifactID)
  override def packageToPublishActions = super.packageToPublishActions ++ Seq(packageSrc)

  override def managedStyle = ManagedStyle.Maven
  def publishUrlSuffix = if (version.toString.endsWith("-SNAPSHOT")) "snapshots/" else "releases/"
  val publishTo = "Scala Tools Nexus" at "http://nexus.scala-tools.org/content/repositories/" + publishUrlSuffix

  // Lift Libraries
  val liftMongoRecord = "net.liftweb" %% "lift-mongodb-record" % liftVersion.value.toString

  // Java Libraries
  lazy val specsVersion = buildScalaVersion match {
    case "2.8.0" => "1.6.5"
    case _       => "1.6.7.2"
  }
  val junit = "junit"                    % "junit" % "4.8.2"      % "test" withSources()
  val specs = "org.scala-tools.testing" %% "specs" % specsVersion % "test" withSources()

  val snapshots = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"
  val bryanjswift = "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/"
  val junitInterface = "com.novocode" % "junit-interface" % "0.6" % "test"
  override def testFrameworks = super.testFrameworks ++ List(new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker"))
}

protected trait Credential extends BasicManagedProject {

  lazy val ivyCredentials   = Path.userHome / ".ivy2" / ".credentials"
  lazy val mavenCredentials = Path.userHome / ".m2"   / "settings.xml"

  lazy val scalaTools = ("Sonatype Nexus Repository Manager", "nexus.scala-tools.org")

  (ivyCredentials.asFile, mavenCredentials.asFile) match {
    case (ivy, _) if ivy.canRead => Credentials(ivy, log)
    case (_, mvn) if mvn.canRead => loadMavenCredentials(mvn)
    case _ => log.warn("Could not read any of the settings files %s or %s".format(ivyCredentials, mavenCredentials))
  }

  private def loadMavenCredentials(file: java.io.File) {
    try {
      xml.XML.loadFile(file) \ "servers" \ "server" foreach (s => {
        val host = (s \ "id").text
        val realm = if (host == scalaTools._2) scalaTools._1 else "Unknown"
        Credentials.add(realm, host, (s \ "username").text, (s \ "password").text)
      })
    } catch {
      case e => log.warn("Could not read the settings file %s [%s]".format(file, e.getMessage))
    }
  }

}
