resolvers += Resolver.url("scalasbt", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))  (Resolver.ivyStylePatterns)

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")

addSbtPlugin("com.foursquare" % "spindle-codegen-plugin" % "3.0.0-M1.2")

scalacOptions ++= Seq("-deprecation", "-unchecked")
