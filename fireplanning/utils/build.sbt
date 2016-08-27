scalaVersion := "2.10.5"

//resolvers += "Local Maven" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

// additional libraries
libraryDependencies ++= Seq(
"ai.h2o" % "sparkling-water-core_2.10" % "1.6.5",
"com.ibm" % "gpu-enabler_2.10" % "1.0.0"
)
