// ✅ Attach correlated COUNT aggregates as wide columns WITHOUT ambiguous key names
private def attachCountAggs(graph: TableGraph, wideDf: DataFrame, countAggsRaw: Map[String, Set[String]]): DataFrame = {
  if (countAggsRaw.isEmpty) return wideDf

  // Canonicalize table names to those present in graph (case-insensitive)
  val countAggs: Map[String, Set[String]] =
    countAggsRaw.toSeq
      .groupBy { case (t, _) => graph.tables.keys.find(_.equalsIgnoreCase(t)).getOrElse(t) }
      .map { case (canonT, entries) => canonT -> entries.flatMap(_._2).toSet }

  // case-insensitive resolve in wide DF (kept local for this function)
  def resolveWideColCI(df: DataFrame, name0: String): Column = {
    val cols = df.columns
    val lowerToActual: Map[String, String] = cols.groupBy(_.toLowerCase).map { case (k, vs) => k -> vs.head }

    val name = name0.trim
    val nameL = name.toLowerCase
    val tail = name.split('.').last
    val tailL = tail.toLowerCase
    def litCol(actual: String): Column = col(s"`$actual`")

    // Prefer exact match first, then case-insensitive name, then unique dotted match
    cols.find(_ == name)
      .orElse(lowerToActual.get(nameL))
      .orElse(lowerToActual.get(tailL))
      .map(litCol)
      .getOrElse {
        val dottedMatches = cols.filter(c => c.toLowerCase.endsWith("." + tailL))
        dottedMatches.length match {
          case 1 => litCol(dottedMatches.head)
          case 0 => throw new IllegalArgumentException(s"Cannot resolve wide column '$name0' (case-insensitive).")
          case _ => throw new IllegalArgumentException(s"Ambiguous wide column '$tail'. Matches: ${dottedMatches.mkString(", ")}")
        }
      }
  }

  countAggs.foldLeft(wideDf) { case (curWide, (tableName, colsToCount)) =>
    val node = graph.tables.getOrElse(tableName, throw new IllegalArgumentException(s"COUNT references unknown source table '$tableName'."))

    colsToCount.foldLeft(curWide) { case (wide2, colToCount) =>
      // resolve actual column name in that source DF (case-insensitive)
      val srcActualOpt = node.df.columns.find(_.equalsIgnoreCase(colToCount))
      if (srcActualOpt.isEmpty) throw new IllegalArgumentException(s"COUNT references missing column '$colToCount' in '$tableName'.")
      val srcActual = srcActualOpt.get

      // IMPORTANT:
      // - Use a UNIQUE key column name in agg DF to avoid any ambiguity with wide DF columns.
      val aggKeyName = s"__aggKey__${tableName}__${srcActual}"
      val aggColName = s"__agg__${tableName}__count__${srcActual}"

      val aggDf =
        node.df
          .select(col(s"`$srcActual`").as(aggKeyName))
          .groupBy(col(s"`$aggKeyName`"))
          .agg(count(lit(1)).cast("long").as(aggColName))

      val wideJoinKey = resolveWideColCI(wide2, srcActual) // current row value of that column

      wide2
        .join(aggDf, wideJoinKey === aggDf.col(s"`$aggKeyName`"), "left")
        .drop(s"`$aggKeyName`")
    }
  }
}
