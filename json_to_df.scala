
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// 1. Read all JSON files
val df = spark.read
  .option("multiLine", "true")
  .option("mode", "PERMISSIVE")
  .json(s"$filesPath/*.{JSON,json}")

// 2. Reusable function to expand nested arrays
def expandArray(
    source: DataFrame,
    arrayName: String,
    keys: Seq[String],
    idName: String
): DataFrame = {

  val tmp = source.select(
    (keys.map(col) :+
      posexplode(col(arrayName)).as(Seq("pos", "Item"))): _*
  )
  .filter(col("Item").isNotNull)

  val ids = keys.map(col) :+
    (col("pos") + 1).alias(idName)

  tmp.schema("Item").dataType match {

    case _: StructType =>
      tmp.select((ids :+ col("Item.*")): _*)

    case StringType if arrayName == "Witnesses" =>
      val jsonStrings = tmp
        .filter(trim(col("Item")).startsWith("{"))
        .select("Item")
        .as[String]

      if (jsonStrings.take(1).nonEmpty) {
        val schema = spark.read.json(jsonStrings).schema

        tmp.select(
          (ids :+
            from_json(col("Item"), schema).alias("Parsed")): _*
        ).select(
          (keys.map(col) :+
            col(idName) :+
            col("Parsed.*")): _*
        )
      } else {
        tmp.select((ids :+ col("Item").alias(arrayName)): _*)
      }

    case _ =>
      tmp.select((ids :+ col("Item").alias(arrayName)): _*)
  }
}

// 3. Crash
val crash_df_init = df
  .drop("Units", "Witnesses")
  .withColumnRenamed("DocumentNumber", "Crash_ID")

// 4. Units
val units_df_init = expandArray(
  df.withColumnRenamed("DocumentNumber", "Crash_ID"),
  "Units",
  Seq("Crash_ID"),
  "Unit_Id"
)

// 5. Witnesses
val witnesses_df_init = expandArray(
  df.withColumnRenamed("DocumentNumber", "Crash_ID"),
  "Witnesses",
  Seq("Crash_ID"),
  "Witness_Id"
)

// 6. DamagedAreas
val damagedareas_df_init = expandArray(
  units_df_init,
  "DamagedAreas",
  Seq("Crash_ID", "Unit_Id"),
  "DamagedAreas_Id"
)

// 7. SequenceOfEvents
val sequenceofevents_df_init = expandArray(
  units_df_init,
  "SequenceOfEvents",
  Seq("Crash_ID", "Unit_Id"),
  "SequenceOfEvents_Id"
)

// 8. People
val people_df_init = expandArray(
  units_df_init,
  "People",
  Seq("Crash_ID", "Unit_Id"),
  "People_Id"
)

// 9. Endorsements
val endorsements_df_init = expandArray(
  people_df_init,
  "Endorsements",
  Seq("Crash_ID", "Unit_Id", "People_Id"),
  "Endorsements_Id"
)

// 10. Restrictions (source JSON uses singular)
val restrictions_df_init = expandArray(
  people_df_init,
  "Restriction",
  Seq("Crash_ID", "Unit_Id", "People_Id"),
  "Restrictions_Id"
)

// 11. DrugTestResult
val drugtestresult_df_init = expandArray(
  people_df_init,
  "DrugTestResult",
  Seq("Crash_ID", "Unit_Id", "People_Id"),
  "DrugTestResult_Id"
)
