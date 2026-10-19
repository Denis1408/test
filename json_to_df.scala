
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Renumber IDs within each Crash_ID
def renumberIds(
    df: DataFrame,
    idCol: String,
    orderCols: String*
): DataFrame = {
  val w = Window.partitionBy("Crash_ID")
    .orderBy(orderCols.map(col): _*)

  df.withColumn(idCol, row_number().over(w))
}

// 1. Crash
val crash_df_init = df
  .drop("Units", "Witnesses")
  .withColumnRenamed("DocumentNumber", "Crash_ID")

// 2. Units
val units_df_init = expandArray(
  df.withColumnRenamed("DocumentNumber", "Crash_ID"),
  "Units", Seq("Crash_ID"), "Unit_Id"
)

// 3. Witnesses
val witnesses_df_init = expandArray(
  df.withColumnRenamed("DocumentNumber", "Crash_ID"),
  "Witnesses", Seq("Crash_ID"), "Witness_Id"
)

// 4. DamagedAreas
val damagedareas_df_init = renumberIds(
  expandArray(units_df_init, "DamagedAreas",
    Seq("Crash_ID", "Unit_Id"), "DamagedAreas_Id"),
  "DamagedAreas_Id", "Unit_Id", "DamagedAreas_Id"
)

// 5. SequenceOfEvents
val sequenceofevents_df_init = renumberIds(
  expandArray(units_df_init, "SequenceOfEvents",
    Seq("Crash_ID", "Unit_Id"), "SequenceOfEvents_Id"),
  "SequenceOfEvents_Id", "Unit_Id", "SequenceOfEvents_Id"
)

// 6. People - assign final People_Id first
val people_df_init = renumberIds(
  expandArray(units_df_init, "People",
    Seq("Crash_ID", "Unit_Id"), "People_Id"),
  "People_Id", "Unit_Id", "People_Id"
)

// 7. Endorsements - inherit correct People_Id
val endorsements_df_init = renumberIds(
  expandArray(people_df_init, "Endorsements",
    Seq("Crash_ID", "Unit_Id", "People_Id"),
    "Endorsements_Id"),
  "Endorsements_Id",
  "People_Id", "Endorsements_Id"
)

// 8. Restrictions - inherit correct People_Id
val restrictions_df_init = renumberIds(
  expandArray(people_df_init, "Restriction",
    Seq("Crash_ID", "Unit_Id", "People_Id"),
    "Restrictions_Id"),
  "Restrictions_Id",
  "People_Id", "Restrictions_Id"
)

// 9. DrugTestResult - inherit correct People_Id
val drugtestresult_df_init = renumberIds(
  expandArray(people_df_init, "DrugTestResult",
    Seq("Crash_ID", "Unit_Id", "People_Id"),
    "DrugTestResult_Id"),
  "DrugTestResult_Id",
  "People_Id", "DrugTestResult_Id"
)



val resultDFs = Seq(
  "Crash" -> crash_df_init,
  "Units" -> units_df_init,
  "Witnesses" -> witnesses_df_init,
  "DamagedAreas" -> damagedareas_df_init,
  "SequenceOfEvents" -> sequenceofevents_df_init,
  "People" -> people_df_init,
  "Endorsements" -> endorsements_df_init,
  "Restrictions" -> restrictions_df_init,
  "DrugTestResult" -> drugtestresult_df_init
)

resultDFs.foreach { case (name, data) =>
  println(s"$name: ${data.count()} rows")
}

display(people_df_init)



