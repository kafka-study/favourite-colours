name := "favourite-colours"
organization:= "com.hiroshisilva.favourite.colours.kafka.streams"
version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
  "org.slf4j" %  "slf4j-api" % "1.7.25",
  "org.slf4j" %  "slf4j-log4j12" % "1.7.25"
)

// leverage java 8
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalacOptions := Seq("-target:jvm-1.8")
initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}