resolvers += Resolver.url("scalasbt", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))  (Resolver.ivyStylePatterns)

addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")

addSbtPlugin("com.foursquare" % "spindle-codegen-plugin" % "3.0.0-M4.2")

scalacOptions ++= Seq("-deprecation", "-unchecked")
