
import java.util.UUID

val rand_id = UUID.randomUUID().toString

val file_name = s"output_parquet_$rand_id.csv"

val tempPath = s"abfss://$container@$fqdn/$folder/temp_$rand_id"
val outputPath = s"abfss://$container@$fqdn/$folder/$file_name"

// Force single partition = single CSV part file
resultDF_all(dest_table_name)
  .coalesce(1)
  .write
  .format("csv")
  .option("header", "true")
  .option("delimiter", ";")
  .option("quote", "\"")
  .option("escape", "\"")
  .option("quoteAll", "false")
  .option("nullValue", "")
  .option("encoding", "UTF-8")
  .mode("overwrite")
  .save(tempPath)

// Find the single generated CSV
val csvFiles = dbutils.fs.ls(tempPath)
  .filter(f => f.name.startsWith("part-") && f.name.endsWith(".csv"))

require(csvFiles.length == 1,
  s"Expected exactly one CSV file, found ${csvFiles.length}")

// Move and rename it
dbutils.fs.mv(csvFiles.head.path, outputPath)

// Remove Spark temporary files
dbutils.fs.rm(tempPath, true)

// Store the actual filename
files_data += ((dest_table_name + "|" + file_name))

println(s"Created: $outputPath")
