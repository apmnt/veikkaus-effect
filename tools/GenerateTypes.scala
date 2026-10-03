//> using scala "3.9.0"
//> using jvm "26"
//> using dep "com.thesamet.scalapb:scalapbc_2.13:0.11.20"

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.util.Using

object GenerateTypes:
  def main(args: Array[String]): Unit =
    val root = Path.of(".").toAbsolutePath.normalize()
    val schema = root.resolve("proto/odds/v1/odds_observation.proto")
    require(Files.isRegularFile(schema), "Run this command from the project root.")

    val output = root.resolve("generated/scala")
    val includes = root.resolve(".scala-build/protobuf-includes")
    val timestamp = includes.resolve("google/protobuf/timestamp.proto")
    Files.createDirectories(output)
    Files.createDirectories(timestamp.getParent)

    // The standard Timestamp schema is bundled with the compiler's dependency.
    // Extract it locally, so no system protoc installation is required.
    val resource = Option(
      classOf[com.google.protobuf.Timestamp].getResourceAsStream("/google/protobuf/timestamp.proto")
    ).getOrElse(throw new IllegalStateException("Missing bundled Timestamp schema."))
    Using.resource(resource) { input =>
      Files.copy(input, timestamp, StandardCopyOption.REPLACE_EXISTING)
    }

    // ScalaPBC manages the pinned protoc binary and runs ScalaPB locally.
    // --throw reports failure as an exception rather than exiting this JVM.
    scalapb.ScalaPBC.main(Array(
      "--throw",
      "-v3.25.8",
      s"--proto_path=${root.resolve("proto")}",
      s"--proto_path=$includes",
      s"--scala_out=flat_package,no_lenses:$output",
      schema.toString
    ))

    println(s"Generated Scala types in $output")
