// Source DF
var resultDF_cid =
  outMap(dest_table_name)
    .withColumn("ArchDateTime", current_timestamp())

// Target DF
val targetDF =
  spark.read
    .jdbc(jdbcUrl2, target_table, connectionProperties2)
    .limit(1)

// Case-insensitive source column lookup
val sourceColsLower =
  resultDF_cid.columns.map(_.toLowerCase).toSet

// Find target columns missing from source, case-insensitive
val missingTargetCols =
  targetDF.columns.filterNot(c => sourceColsLower.contains(c.toLowerCase))

// Add missing target columns using TARGET column names/casing
val srcDFExtended =
  missingTargetCols.foldLeft(resultDF_cid) { (df, colName) =>
    df.withColumn(colName, lit(null).cast("string"))
  }

// Build a case-insensitive lookup from source/extended columns
val srcColLookup =
  srcDFExtended.columns
    .groupBy(_.toLowerCase)
    .map { case (k, v) => k -> v.head }

// Select columns in exact TARGET order,
// but resolve source columns case-insensitively
val cleanedDF =
  srcDFExtended.select(
    targetDF.columns.map { targetCol =>
      val actualSourceCol = srcColLookup(targetCol.toLowerCase)
      col(s"`$actualSourceCol`").as(targetCol)
    }: _*
  )
