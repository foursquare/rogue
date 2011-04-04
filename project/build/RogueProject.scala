import sbt._
import sbt.Process._
import java.lang.System
import java.util.concurrent.{Callable, Executors}

class RogueProject(info: ProjectInfo) extends DefaultProject(info) {
  val liftVersion = property[Version]

  override def packageSrcJar = defaultJarPath("-sources.jar")
  val sourceArtifact = Artifact.sources(artifactID)
  override def packageToPublishActions = super.packageToPublishActions ++ Seq(packageSrc)

  override def managedStyle = ManagedStyle.Maven
  val publishTo = "Scala Tools Nexus" at "http://nexus.scala-tools.org/content/repositories/releases/"
  Credentials(Path.userHome / ".ivy2" / ".credentials", log)

  // Lift Libraries
  val liftMongoRecord = "net.liftweb" %% "lift-mongodb-record" % liftVersion.value.toString withSources()

  // Java Libraries
  lazy val specsVersion = buildScalaVersion match {
    case "2.8.0" => "1.6.5"
    case _       => "1.6.7.2"
  }
  val junit = "junit"                    % "junit" % "4.8.2"      % "test" withSources()
  val specs = "org.scala-tools.testing" %% "specs" % specsVersion % "test" withSources()

  val bryanjswift = "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/"
  val junitInterface = "com.novocode" % "junit-interface" % "0.6" % "test"
  override def testFrameworks = super.testFrameworks ++ List(new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker"))
}
