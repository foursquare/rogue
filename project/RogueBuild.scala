// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
import sbt._

object RogueBuild extends Build {
  override lazy val projects =
    Seq(all, core, lift)

  lazy val all: Project = Project("all", file(".")) aggregate(
    core, lift)

  lazy val core = Project("core", file("rogue-core/"))
  lazy val lift = Project("lift", file("rogue-lift/")) dependsOn(core % "compile;test->test;runtime->runtime")
}
