val folderExists = try {
  dbutils.fs.ls(filesPath)
  println(s"✓ Storage connected and path accessible: $filesPath")
  true

} catch {

  case e: java.io.FileNotFoundException =>
    println(s"✓ Storage connected, but folder does not exist: $filesPath")
    false

  case e: Exception =>
    val msg = Option(e.getMessage).getOrElse("").toLowerCase

    if (
      msg.contains("authentication") ||
      msg.contains("unauthorized") ||
      msg.contains("403") ||
      msg.contains("401") ||
      msg.contains("invalid_client") ||
      msg.contains("client secret") ||
      msg.contains("token")
    ) {
      println(s"✗ ABFSS authentication/connection failed")
      println(s"Error: ${e.getMessage}")
    }
    else {
      println(s"✗ ABFSS access failed")
      println(s"Type: ${e.getClass.getName}")
      println(s"Error: ${e.getMessage}")
    }

    false
}
