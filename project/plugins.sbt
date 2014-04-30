resolvers += Resolver.url("scalasbt", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))  (Resolver.ivyStylePatterns)

addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")

addSbtPlugin("com.foursquare" % "spindle-codegen-plugin" % "1.8.4")

scalacOptions ++= Seq("-deprecation", "-unchecked")
