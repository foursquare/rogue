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
  val liftCommon      = "net.liftweb" %% "lift-common"         % liftVersion.value.toString withSources()
  val liftJson        = "net.liftweb" %% "lift-json"           % liftVersion.value.toString withSources()
  val liftJsonExt     = "net.liftweb" %% "lift-json-ext"       % liftVersion.value.toString withSources()
  val liftRecord      = "net.liftweb" %% "lift-record"         % liftVersion.value.toString withSources()
  val liftUtil        = "net.liftweb" %% "lift-util"           % liftVersion.value.toString withSources()
  val liftMongo       = "net.liftweb" %% "lift-mongodb"        % liftVersion.value.toString withSources()
  val liftMongoRecord = "net.liftweb" %% "lift-mongodb-record" % liftVersion.value.toString withSources()

  lazy val specsVersion = buildScalaVersion match {
    case "2.8.0" => "1.6.5"
    case "2.8.1" => "1.6.7.2"
    case _       => "1.6.8-SNAPSHOT"
  }

  // Scala Libraries
  val scalaCompiler     = "org.scala-lang"           % "scala-compiler"     % scalaVer % "test"
  val specs             = "org.scala-tools.testing" %% "specs" % specsVersion % "test" withSources()

  // Java Libraries
  val mongo        = "org.mongodb"         % "mongo-java-driver" % "2.4"
  val jodaTime     = "joda-time"           % "joda-time"         % "1.6.2" withSources()
  val commons      = "org.apache.commons"  % "commons-math"      % "2.1"
  val junit        = "junit"               % "junit"             % "4.8.2" % "test" withSources()

  val stsnapshot = ScalaToolsSnapshots
  val bryanjswift = "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/" 
  val junitInterface = "com.novocode" % "junit-interface" % "0.6" % "test"
  override def testFrameworks = super.testFrameworks ++ List(new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker")) 
}
