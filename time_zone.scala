spark.conf.set("spark.sql.session.timeZone", "America/New_York")


import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.session.timeZone", "America/New_York")

try {
  tables(tmp_df)
    .withColumn(
      "ArchDateTime",
      date_format(col("ArchDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )
    .write
    .mode("append")
    .format("sqlserver")
    // Keep all your existing options here
    .save()
}
