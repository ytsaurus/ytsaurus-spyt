crossSbtVersions := Seq("1.10.6")

sbtPlugin := true

organization := "tech.ytsaurus.spyt"

name := "YtPublishPlugin"
version := "2.5.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "tech.ytsaurus" % "ytsaurus-client" % "1.2.3" excludeAll (
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  )
)