
import AssemblyKeys._  // put this at the top of the file

assemblySettings

name := "Monitor"

version := "4.8.1.20"

scalaVersion := "2.11.8"

scalaSource in Compile <<= baseDirectory(_ /"src")

libraryDependencies +="com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"

libraryDependencies += "javax.mail" % "mail" % "1.4.1"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.12",
                            "org.slf4j" % "slf4j-simple" % "1.7.12",
                            "org.clapper" %% "grizzled-slf4j" % "1.0.2")

fork in run := true
