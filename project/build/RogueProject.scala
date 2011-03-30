import sbt._
import sbt.Process._
import java.lang.System
import java.util.concurrent.{Callable, Executors}

class RogueProject(info: ProjectInfo) extends DefaultProject(info) {
  val liftVer = "2.2"
  val scalaVer = buildScalaVersion

  override def packageSrcJar = defaultJarPath("-sources.jar")
  val sourceArtifact = Artifact.sources(artifactID)
  override def packageToPublishActions = super.packageToPublishActions ++ Seq(packageSrc)

  override def managedStyle = ManagedStyle.Maven
  val publishTo = "Scala Tools Nexus" at "http://nexus.scala-tools.org/content/repositories/releases/"
  Credentials(Path.userHome / ".ivy2" / ".credentials", log)

  // Lift Libraries
  val liftCommon      = "net.liftweb" %% "lift-common"         % liftVer % "compile" withSources()
  val liftJson        = "net.liftweb" %% "lift-json"           % liftVer % "compile" withSources()
  val liftJsonExt     = "net.liftweb" %% "lift-json-ext"       % liftVer % "compile" withSources()
  val liftRecord      = "net.liftweb" %% "lift-record"         % liftVer % "compile" withSources()
  val liftUtil        = "net.liftweb" %% "lift-util"           % liftVer % "compile" withSources()
  val liftMongo       = "net.liftweb" %% "lift-mongodb"        % liftVer % "compile" withSources()
  val liftMongoRecord = "net.liftweb" %% "lift-mongodb-record" % liftVer % "compile" withSources()

  // Scala Libraries
  val scalaCompiler     = "org.scala-lang"           % "scala-compiler"     % scalaVer % "test"
  val specs             = "org.scala-tools.testing" %% "specs"              % "1.6.5"  % "test" withSources()

  // Java Libraries
  val mongo        = "org.mongodb"         % "mongo-java-driver" % "2.4"
  val jodaTime     = "joda-time"           % "joda-time"         % "1.6" withSources()
  val commons      = "org.apache.commons"  % "commons-math"      % "2.1"
  val junit        = "junit"               % "junit"             % "4.8.2" % "test" withSources()

  val bryanjswift = "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/" 
  val junitInterface = "com.novocode" % "junit-interface" % "0.6" % "test->default"
  override def testFrameworks = super.testFrameworks ++ List(new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker")) 
}
