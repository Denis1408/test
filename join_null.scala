def joinNullSafe(
    left: DataFrame,
    right: DataFrame,
    keys: Seq[String],
    joinType: String = "left"
): DataFrame = {

  val condition = keys
    .map(k => left(k).eqNullSafe(right(k)))
    .reduce(_ && _)

  left.join(right, condition, joinType)
}

case "ADDRESS" =>
  joinNullSafe(
    df,
    tables_ai_init("PERSON_INFO"),
    Seq(
      "Crash_Id",
      "ACC_VEHICLE_Id",
      "OCCUPANT_Id",
      "ACC_NON_MOTORIST_Id"
    ),
    "left"
  )
  .select(
    df("*"),
    tables_ai_init("PERSON_INFO").col("PERSON_INFO_Id")
  )
