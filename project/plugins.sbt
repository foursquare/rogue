resolvers += Resolver.url("scalasbt", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))  (Resolver.ivyStylePatterns)

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")

addSbtPlugin("com.foursquare" % "spindle-codegen-plugin" % "1.8.3")

scalacOptions ++= Seq("-deprecation", "-unchecked")
