name := "ContribsGH2-Z"

version := "0.1"

organization := "com.avargasv"

scalaVersion := "2.13.10"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "dev.zio"           %% "zio"                  % "2.0.10",
  "dev.zio"           %% "zio-http"             % "3.0.0-RC1",
  "dev.zio"           %% "zio-json"             % "0.5.0",
  "ch.qos.logback"    %  "logback-classic"      % "1.2.11",
  "com.github.kstyrc" %  "embedded-redis"       % "0.6",
  "redis.clients"     %  "jedis"                % "3.2.0"
)
