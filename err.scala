val rootPath =
  s"abfss://$container@$storageAccount.dfs.core.windows.net/"

val storageConnected = try {
  dbutils.fs.ls(rootPath)

  println(s"✓ Successfully connected to ABFSS: $rootPath")
  true

} catch {
  case e: Exception =>
    println(s"✗ Cannot connect to ABFSS: $rootPath")
    println(s"Type: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    false
}

if (!storageConnected) {
  dbutils.notebook.exit("ABFSS storage connection failed")
}

val folderExists = try {
  dbutils.fs.ls(filesPath)
  true
} catch {
  case _: java.io.FileNotFoundException => false

  case e: Exception
      if Option(e.getMessage)
        .exists(_.toLowerCase.contains("no such file or directory")) =>
    false

  case e: Exception =>
    println(s"Unexpected error checking folder:")
    println(s"${e.getClass.getName}: ${e.getMessage}")
    throw e
}

/*------------------------------------------------------*/

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
