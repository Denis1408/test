val sourceDfs: Map[String, DataFrame] = Map(
  "Unit" -> unitDf,
  "Summary" -> summaryDf,
  "Person_Injured" -> personInjuredDf
)

val tableLoader: String => DataFrame = (t: String) => {
  sourceDfs.getOrElse(
    t,
    sourceDfs.collectFirst { case (k, df) if k.equalsIgnoreCase(t) => df }
      .getOrElse(throw new IllegalArgumentException(s"Source DF not provided for table: $t. Available: ${sourceDfs.keys.mkString(", ")}"))
  )
}


val outMap = MappingAutomation.buildAllDestTablesAsMap(
  spark = spark,
  mappingDf = mappingDf,
  pkByTable = pkByTable,
  joinEdgesRaw = joinEdges,
  sourceTablePriority = sourceTablePriority,
  tableLoader = tableLoader
)
