// =================================================================================================
// FINAL ONE-CELL SCRIPT (BASED ON YOUR test23.scala) + ✅ ADDED TYPE-2 COUNT CONDITION LOGIC
// -------------------------------------------------------------------------------------------------
// NEW FEATURE ADDED:
// ✅ Supports COUNT expressions inside Condition for type 2 (and type 1/2 boolean conditions in general)
//
// Example mapping condition now supported:
//   Unit.COUNT(Crash_Id) == 1
//
// Meaning (CORRELATED COUNT):
//   Count rows in SOURCE table "Unit" where Unit.Crash_Id == current row Crash_Id,
//   then compare that count to 1.
//
// Implementation approach:
// 1) Detect COUNT patterns in Condition: <Table>.COUNT(<Column>)
// 2) Precompute aggregate column(s) per referenced source table:
//      __agg__Unit__count__Crash_Id = count(*) grouped by Crash_Id
// 3) Join those aggregates into the wide DF (using a unique join key name to avoid ambiguity)
// 4) Rewrite the condition string:
//      Unit.COUNT(Crash_Id)  ->  __agg__Unit__count__Crash_Id
//    then parse as normal boolean condition.
//
// IMPORTANT: This avoids the Spark ambiguity error by never joining on a column named Crash_Id.
// We join using a unique alias key like: __aggKey__Unit__Crash_Id
// =================================================================================================

import org.apache.spark.sql.{DataFrame, Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.util.Try
import scala.util.matching.Regex

// -------------------------------------------------------------------------------------------------
// Data model for joining source tables into a "wide" DataFrame
// -------------------------------------------------------------------------------------------------
case class JoinEdge(
  leftTable: String,
  rightTable: String,
  leftKeys: Seq[String],
  rightKeys: Seq[String],
  joinType: String = "left"
)

// -------------------------------------------------------------------------------------------------
// TableGraph = loaded source DFs + join edges between them
// -------------------------------------------------------------------------------------------------
case class TableNode(df: DataFrame, pk: Seq[String])
case class TableGraph(tables: Map[String, TableNode], edges: Seq[JoinEdge])

// -------------------------------------------------------------------------------------------------
// Normalized rule representation extracted from mappingDf row
// -------------------------------------------------------------------------------------------------
case class MapRule(
  destColumn: String,
  sourceTable: Option[String],
  sourceColumn: Option[String],
  sourceValueRaw: Option[String],
  destValueRaw: Option[String],
  destValueDescRaw: Option[String],
  conversionType: Int,
  conditionRaw: Option[String],
  priority: Int
)

// =================================================================================================
// 1) CONDITION SANITIZER
// =================================================================================================
object ConditionSanitizer {
  def sanitize(raw: String): String = {
    if (raw == null) return null
    var s = raw.trim

    s = s.replace('\u201C','"').replace('\u201D','"').replace('\u2018','\'').replace('\u2019','\'')
    while (s.endsWith(";")) s = s.dropRight(1).trim

    val trailingBad = Seq("AND","OR","&&","||","(")
    var changed = true
    while (changed) {
      changed = false
      val up = s.toUpperCase
      trailingBad.find(tok => up.endsWith(tok)) match {
        case Some(tok) =>
          s = s.dropRight(tok.length).trim
          changed = true
        case None =>
      }
    }

    val open = s.count(_ == '(')
    val close = s.count(_ == ')')
    if (open > close) s = s + (")" * (open - close))

    s
  }
}

// =================================================================================================
// 2) CONDITION PARSER
// =================================================================================================
object ConditionParser {

  sealed trait Tok { def s: String }
  case class ID(s: String) extends Tok
  case class STR(s: String) extends Tok
  case class NUM(s: String) extends Tok
  case class OP(s: String) extends Tok
  case class LP(s: String = "(") extends Tok
  case class RP(s: String = ")") extends Tok
  case class COMMA(s: String = ",") extends Tok

  private val twoCharOps = Set("==","!=", "<>","&&","||","<=",">=")
  private val oneCharOps = Set("=","<",">","!")
  private val keywords  = Set("AND","OR","IN","NOT","IS","NULL")

  def parseToColumn(expr: String, colResolver: String => Column): Column = {
    val tokens = tokenize(expr)
    val p = new Parser(tokens, colResolver)
    val c = p.parseExpr()
    p.expectEnd()
    c
  }

  private def tokenize(input0: String): Vector[Tok] = {
    val input = input0.trim
    val n = input.length
    var i = 0
    val out = Vector.newBuilder[Tok]

    def isIdStart(ch: Char) = ch.isLetter || ch == '_'
    def isIdPart(ch: Char)  = ch.isLetterOrDigit || ch == '_' || ch == '.'

    while (i < n) {
      input.charAt(i) match {
        case ch if ch.isWhitespace => i += 1
        case '(' => out += LP(); i += 1
        case ')' => out += RP(); i += 1
        case ',' => out += COMMA(); i += 1

        case '"' | '\'' =>
          val quote = input.charAt(i); i += 1
          val start = i
          while (i < n && input.charAt(i) != quote) i += 1
          out += STR(input.substring(start, i))
          if (i < n && input.charAt(i) == quote) i += 1

        case ch if ch.isDigit =>
          val start = i
          while (i < n && (input.charAt(i).isDigit || input.charAt(i) == '.')) i += 1
          out += NUM(input.substring(start, i))

        case ch if isIdStart(ch) =>
          val start = i; i += 1
          while (i < n && isIdPart(input.charAt(i))) i += 1
          val raw = input.substring(start, i)
          val up  = raw.toUpperCase
          if (keywords.contains(up)) out += OP(up) else out += ID(raw)

        case _ =>
          if (i + 1 < n && twoCharOps.contains(input.substring(i, i + 2))) { out += OP(input.substring(i, i + 2)); i += 2 }
          else if (oneCharOps.contains(input.charAt(i).toString)) { out += OP(input.charAt(i).toString); i += 1 }
          else throw new IllegalArgumentException(s"Unexpected character '${input.charAt(i)}' at position $i in: $input0")
      }
    }
    out.result()
  }

  private final class Parser(tokens: Vector[Tok], colResolver: String => Column) {
    private var pos = 0
    private def peek: Option[Tok] = if (pos < tokens.length) Some(tokens(pos)) else None
    private def peek2: Option[Tok] = if (pos + 1 < tokens.length) Some(tokens(pos + 1)) else None
    private def next(): Tok = { val t = tokens(pos); pos += 1; t }

    def expectEnd(): Unit =
      if (pos != tokens.length) throw new IllegalArgumentException(s"Unexpected token at end: ${tokens(pos).s}")

    def parseExpr(): Column = parseOr()

    private def parseOr(): Column = {
      var left = parseAnd()
      while (peek.exists { case OP("OR") | OP("||") => true; case _ => false }) { next(); left = left.or(parseAnd()) }
      left
    }

    private def parseAnd(): Column = {
      var left = parsePred()
      while (peek.exists { case OP("AND") | OP("&&") => true; case _ => false }) { next(); left = left.and(parsePred()) }
      left
    }

    private def parsePred(): Column = peek match {
      case Some(LP(_)) => next(); val inner = parseExpr(); expectRP(); inner
      case _           => parseComparison()
    }

    private def parseComparison(): Column = {
      val leftTerm = parseTerm()
      peek match {
        case Some(OP("=" | "==")) if peek2.contains(OP("NULL")) => next(); next(); leftTerm.isNull
        case Some(OP("!=" | "<>")) if peek2.contains(OP("NULL")) => next(); next(); leftTerm.isNotNull

        case Some(OP("=" | "==")) => next(); leftTerm === parseTerm()
        case Some(OP("!=" | "<>")) => next(); leftTerm =!= parseTerm()
        case Some(OP("<"))  => next(); leftTerm <  parseTerm()
        case Some(OP(">"))  => next(); leftTerm >  parseTerm()
        case Some(OP("<=")) => next(); leftTerm <= parseTerm()
        case Some(OP(">=")) => next(); leftTerm >= parseTerm()

        case Some(OP("IS")) =>
          next()
          val not = peek.contains(OP("NOT"))
          if (not) next()
          expectOp("NULL")
          if (not) leftTerm.isNotNull else leftTerm.isNull

        case Some(OP("NOT")) =>
          next(); expectOp("IN"); val values = parseInList(); !leftTerm.isin(values: _*)

        case Some(OP("IN")) =>
          next(); val values = parseInList(); leftTerm.isin(values: _*)

        case _ => leftTerm
      }
    }

    private def parseInList(): Seq[Any] = {
      expectLP()
      val vals = scala.collection.mutable.ArrayBuffer.empty[Any]
      vals += parseLiteralAny()
      while (peek.exists(_.isInstanceOf[COMMA])) { next(); vals += parseLiteralAny() }
      expectRP()
      vals.toSeq
    }

    private def parseTerm(): Column = peek match {
      case Some(ID(name))   => next(); colResolver(name)
      case Some(STR(v))     => next(); lit(v)
      case Some(NUM(v))     => next(); Try(v.toLong).map(lit).getOrElse(Try(v.toDouble).map(lit).getOrElse(lit(v)))
      case Some(OP("NULL")) => next(); lit(null)
      case other            => throw new IllegalArgumentException(s"Expected term but found: ${other.map(_.s)}")
    }

    private def parseLiteralAny(): Any = peek match {
      case Some(STR(v))     => next(); v
      case Some(NUM(v))     => next(); Try(v.toLong).getOrElse(Try(v.toDouble).getOrElse(v))
      case Some(OP("NULL")) => next(); null
      case other            => throw new IllegalArgumentException(s"Expected literal but found: ${other.map(_.s)}")
    }

    private def expectOp(value: String): Unit = peek match {
      case Some(OP(v)) if v == value => next()
      case _ => throw new IllegalArgumentException(s"Expected '$value'")
    }
    private def expectLP(): Unit = peek match {
      case Some(LP(_)) => next()
      case _ => throw new IllegalArgumentException("Expected '('")
    }
    private def expectRP(): Unit = peek match {
      case Some(RP(_)) => next()
      case _ => throw new IllegalArgumentException("Expected ')'")
    }
  }
}

// =================================================================================================
// 3) RULE DEPENDENCY EXTRACTION + ✅ COUNT SUPPORT
// =================================================================================================
object RuleDependency {

  // Table.Column references (used to decide what columns to bring into wide df)
  private val tableColRx: Regex =
    """([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)""".r

  // ✅ New: Table.COUNT(Column) references (case-insensitive)
  private val tableCountRx: Regex =
    """(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*COUNT\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)""".r

  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

  // Existing method kept (still used by wide builder for normal columns)
  def requiredTableColsForDest(mappingDf: DataFrame, destTable: String): Map[String, Set[String]] = {
    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTable))
      .select("SourceTableName", "SourceColumnName", "Condition")
      .collect()

    val acc = scala.collection.mutable.Map.empty[String, scala.collection.mutable.Set[String]]
    def add(t: String, c: String): Unit = acc.getOrElseUpdate(t, scala.collection.mutable.Set.empty) += c

    rows.foreach { r =>
      val st = cleanOpt(r.getAs[Any]("SourceTableName"))
      val sc = cleanOpt(r.getAs[Any]("SourceColumnName"))
      val cond = Option(r.getAs[Any]("Condition")).map(_.toString).getOrElse("")

      for (t <- st; c <- sc) add(t, c)

      tableColRx.findAllMatchIn(cond).foreach(m => add(m.group(1), m.group(2)))
    }

    acc.view.mapValues(_.toSet).toMap
  }

  // ✅ New: extract COUNT aggregates needed for a dest table
  // Returns: Map[SourceTable -> Set[ColumnToCountBy]]
  // Example: Unit.COUNT(Crash_Id) -> Map("Unit" -> Set("Crash_Id"))
  def requiredCountAggsForDest(mappingDf: DataFrame, destTable: String): Map[String, Set[String]] = {
    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTable))
      .select("Condition")
      .collect()

    val acc = scala.collection.mutable.Map.empty[String, scala.collection.mutable.Set[String]]
    def add(t: String, c: String): Unit = acc.getOrElseUpdate(t, scala.collection.mutable.Set.empty) += c

    rows.foreach { r =>
      val cond = Option(r.getAs[Any]("Condition")).map(_.toString).getOrElse("")
      tableCountRx.findAllMatchIn(cond).foreach { m =>
        add(m.group(1), m.group(2))
      }
    }
    acc.view.mapValues(_.toSet).toMap
  }

  // ✅ Rewrite: Table.COUNT(Col) -> __agg__Table__count__Col
  // This makes it parseable by our ConditionParser as a column reference.
  def rewriteCountFunctions(cond: String): String = {
    if (cond == null) return null
    tableCountRx.replaceAllIn(cond, m => s"__agg__${m.group(1)}__count__${m.group(2)}")
  }
}

// =================================================================================================
// 4) WIDE BUILDER + ✅ COUNT AGG JOIN
// =================================================================================================
object WideBuilder {

  private def canonicalTableName(graph: TableGraph, t: String): String =
    graph.tables.keys.find(_.equalsIgnoreCase(t))
      .getOrElse(throw new IllegalArgumentException(s"Unknown table '$t'. Known: ${graph.tables.keys.toSeq.sorted.mkString(", ")}"))

  private def selectWithAliases(
    tableName: String,
    df: DataFrame,
    cols: Set[String],
    keepKeys: Seq[String],
    keyPrefix: Option[String]
  ): DataFrame = {

    val keys = keepKeys.distinct.filter(df.columns.contains)
    val dataCols = cols.filter(df.columns.contains).toSeq.distinct.sorted

    val keySelect: Seq[Column] = keyPrefix match {
      case None       => keys.map(col)
      case Some(pfx)  => keys.map(k => col(k).as(s"${pfx}${k}"))
    }

    val dataSelect: Seq[Column] = dataCols.map(c => col(c).as(s"$tableName.$c"))
    val sel = keySelect ++ dataSelect

    if (sel.isEmpty) df else df.select(sel: _*)
  }

  private def buildAdj(edges: Seq[JoinEdge]): Map[String, Seq[JoinEdge]] =
    edges.flatMap(e => Seq(e.leftTable -> e, e.rightTable -> e))
      .groupBy(_._1).view.mapValues(_.map(_._2)).toMap

  private def findPath(edges: Seq[JoinEdge], from: String, to: String): Seq[JoinEdge] = {
    if (from == to) return Seq.empty
    val adj = buildAdj(edges)
    val q = scala.collection.mutable.Queue[String](from)
    val prevEdge = scala.collection.mutable.Map[String, JoinEdge]()
    val prevNode = scala.collection.mutable.Map[String, String]()
    val seen = scala.collection.mutable.Set[String](from)

    while (q.nonEmpty && !seen.contains(to)) {
      val cur = q.dequeue()
      adj.getOrElse(cur, Nil).foreach { e =>
        val nxt = if (e.leftTable == cur) e.rightTable else e.leftTable
        if (!seen.contains(nxt)) {
          seen += nxt
          prevEdge(nxt) = e
          prevNode(nxt) = cur
          q.enqueue(nxt)
        }
      }
    }

    if (!seen.contains(to)) throw new IllegalArgumentException(s"No join path from $from to $to")

    val path = scala.collection.mutable.ArrayBuffer.empty[JoinEdge]
    var cur = to
    while (cur != from) { path += prevEdge(cur); cur = prevNode(cur) }
    path.reverse.toSeq
  }

  private def edgeKeys(from: String, to: String, e: JoinEdge): (Seq[String], Seq[String]) = {
    if (e.leftTable == from && e.rightTable == to) (e.leftKeys, e.rightKeys)
    else if (e.rightTable == from && e.leftTable == to) (e.rightKeys, e.leftKeys)
    else throw new IllegalArgumentException(s"Edge does not connect $from -> $to : $e")
  }

  // Case-insensitive resolver for wide DF columns (returns ONE actual column)
  private def resolveWideColCI(df: DataFrame, name0: String): Column = {
    val cols = df.columns
    val lowerToActual: Map[String, String] = cols.groupBy(_.toLowerCase).map { case (k, vs) => k -> vs.head }

    val name = name0.trim
    val nameL = name.toLowerCase
    val tail = name.split('.').last
    val tailL = tail.toLowerCase
    def litCol(actual: String): Column = col(s"`$actual`")

    cols.find(_ == name) // exact case match preferred
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

  // ✅ Join correlated COUNT aggregates into wideDf
  // For each (Table, Col): build aggDf = count(*) grouped by Col; then join to wide on current-row Col value.
  // Uses a unique join key name to avoid Spark ambiguity errors.
  private def attachCountAggs(graph: TableGraph, wideDf: DataFrame, countAggsRaw: Map[String, Set[String]]): DataFrame = {
    if (countAggsRaw.isEmpty) return wideDf

    // canonicalize table names to those loaded in the graph
    val countAggs: Map[String, Set[String]] =
      countAggsRaw.toSeq
        .groupBy { case (t, _) => graph.tables.keys.find(_.equalsIgnoreCase(t)).getOrElse(t) }
        .map { case (canonT, entries) => canonT -> entries.flatMap(_._2).toSet }

    countAggs.foldLeft(wideDf) { case (curWide, (tableName, colsToCount)) =>
      val node = graph.tables.getOrElse(tableName, throw new IllegalArgumentException(s"COUNT references unknown source table '$tableName'."))

      colsToCount.foldLeft(curWide) { case (wide2, colToCount) =>
        val srcActualOpt = node.df.columns.find(_.equalsIgnoreCase(colToCount))
        if (srcActualOpt.isEmpty) throw new IllegalArgumentException(s"COUNT references missing column '$colToCount' in '$tableName'.")
        val srcActual = srcActualOpt.get

        val aggKeyName = s"__aggKey__${tableName}__${srcActual}"
        val aggColName = s"__agg__${tableName}__count__${srcActual}"

        val aggDf =
          node.df
            .select(col(s"`$srcActual`").as(aggKeyName))
            .groupBy(col(s"`$aggKeyName`"))
            .agg(count(lit(1)).cast("long").as(aggColName))

        val wideJoinKey = resolveWideColCI(wide2, srcActual)

        wide2
          .join(aggDf, wideJoinKey === aggDf.col(s"`$aggKeyName`"), "left")
          .drop(s"`$aggKeyName`")
      }
    }
  }

  // Build wide DF using base table + joins + ✅ optional COUNT aggregates
  def buildWideDf(
    graph: TableGraph,
    mappingDf: DataFrame,
    destTable: String,
    baseTable: String,
    keepAllPk: Boolean,
    countAggsForDest: Map[String, Set[String]] // ✅ NEW PARAM
  ): (DataFrame, String, Set[String]) = {

    val reqRaw = RuleDependency.requiredTableColsForDest(mappingDf, destTable)
    val base = canonicalTableName(graph, baseTable)

    val req: Map[String, Set[String]] =
      reqRaw.toSeq
        .groupBy { case (t, _) => canonicalTableName(graph, t) }
        .map { case (canonT, entries) => canonT -> entries.flatMap(_._2).toSet }

    val neededTables: Seq[String] = (req.keySet + base).toSeq.distinct
    val baseNode = graph.tables(base)

    var wide = selectWithAliases(base, baseNode.df, req.getOrElse(base, Set.empty), baseNode.pk, None)
    val joined = scala.collection.mutable.Set[String](base)

    def getTableDf(t: String): (DataFrame, String, Seq[String]) = {
      val node = graph.tables(t)
      val pfx  = s"__k__${t}__"
      val dfSel = selectWithAliases(t, node.df, req.getOrElse(t, Set.empty), node.pk, Some(pfx))
      (dfSel, pfx, node.pk)
    }

    neededTables.filterNot(_ == base).foreach { target =>
      val pathEdges = findPath(graph.edges, base, target)
      var curTable = base

      pathEdges.foreach { e =>
        val nextTable =
          if (e.leftTable == curTable) e.rightTable
          else if (e.rightTable == curTable) e.leftTable
          else throw new IllegalArgumentException(s"Non-contiguous path at $curTable for edge $e")

        if (!joined.contains(nextTable)) {
          val (lk, rk) = edgeKeys(curTable, nextTable, e)
          val (rightDf, pfx, _) = getTableDf(nextTable)

          val joinCond = lk.zip(rk).map { case (lKey, rKey) =>
            wide.col(lKey) === rightDf.col(s"${pfx}${rKey}")
          }.reduce(_ && _)

          val joinedDf = wide.join(rightDf, joinCond, e.joinType)

          wide =
            if (keepAllPk) joinedDf
            else joinedDf.drop(rk.map(rKey => s"${pfx}${rKey}"): _*)

          joined += nextTable
        }
        curTable = nextTable
      }
    }

    // ✅ Finally attach COUNT aggregates used by mapping conditions
    wide = attachCountAggs(graph, wide, countAggsForDest)

    (wide, base, neededTables.toSet)
  }
}

// =================================================================================================
// 5) DEST TABLE BUILDER  (✅ COUNT rewrite added before parsing)
// =================================================================================================
object DestTableBuilder {

  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

  private def toInt(v: Any, default: Int): Int =
    Option(v).flatMap(x => Try(x.toString.trim.toInt).toOption).getOrElse(default)

  private def isValueToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<VALUE>") || v.equalsIgnoreCase("&lt;VALUE&gt;"))

  private def isFormatToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<FORMAT>") || v.equalsIgnoreCase("&lt;FORMAT&gt;"))

  private def isNullTextToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<NULL>") || v.equalsIgnoreCase("&lt;NULL&gt;"))

  private def isDefaultToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<DEFAULT>") || v.equalsIgnoreCase("&lt;DEFAULT&gt;"))

  private def safeToTimestamp(c: Column): Column = {
    val s = c.cast("string")
    coalesce(
      try_to_timestamp(s, lit("MM/dd/yyyy")),
      try_to_timestamp(s, lit("M/d/yyyy")),
      try_to_timestamp(s, lit("MM-dd-yyyy")),
      try_to_timestamp(s, lit("M-d-yyyy")),
      try_to_timestamp(s, lit("yyyy-MM-dd")),
      try_to_timestamp(s, lit("yyyy/MM/dd")),
      try_to_timestamp(s, lit("yyyyMMdd")),
      try_to_timestamp(s, lit("MM/dd/yyyy HH:mm:ss")),
      try_to_timestamp(s, lit("MM/dd/yyyy H:mm:ss")),
      try_to_timestamp(s, lit("MM/dd/yyyy HH:mm")),
      try_to_timestamp(s, lit("MM/dd/yyyy H:mm")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd HH:mm:ss")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd H:mm:ss")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd HH:mm")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd H:mm")),
      try_to_timestamp(s)
    )
  }

  private def resolver(wideDf: DataFrame, defaultTable: Option[String]): String => Column = {
    val cols = wideDf.columns
    val lowerToActual: Map[String, String] = cols.groupBy(_.toLowerCase).map { case (k, vs) => k -> vs.head }

    (name0: String) => {
      val name = name0.trim
      val nameL = name.toLowerCase
      val tail = name.split('.').last
      val tailL = tail.toLowerCase
      def litCol(actual: String): Column = col(s"`$actual`")

      lowerToActual.get(nameL)
        .orElse(lowerToActual.get(tailL))
        .map(litCol)
        .getOrElse {
          val dottedMatches = cols.filter(c => c.toLowerCase.endsWith("." + tailL))
          dottedMatches.length match {
            case 1 => litCol(dottedMatches.head)
            case 0 => throw new IllegalArgumentException(s"Cannot resolve column '$name0' (case-insensitive).")
            case _ =>
              val preferred = defaultTable.flatMap(t => lowerToActual.get((t + "." + tail).toLowerCase))
              preferred.map(litCol).getOrElse(throw new IllegalArgumentException(s"Ambiguous column '$tail'."))
          }
        }
    }
  }

  private def buildExpr(
    destTableName: String,
    destCol: String,
    rules: Seq[MapRule],
    outDf: DataFrame,
    whenOnlyIfAccNull: Boolean,
    valueSelector: MapRule => Option[String],
    passThroughOnValueToken: Boolean
  ): Column = {

    rules.foldLeft(lit(null).cast("string")) { case (acc, r) =>
      val resRule: String => Column = resolver(outDf, r.sourceTable)

      val fmtRule: Boolean =
        r.conversionType == 1 && isValueToken(r.sourceValueRaw) && isFormatToken(r.destValueRaw)

      val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
        case (None, _) => lit(true)

        case (Some(_), sv) if isValueToken(sv) => lit(true)

        case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true)
        case (Some(_), None) if r.conversionType == 2 => lit(true) // Type2 NULL => wildcard

        case (Some(sc), None) => resRule(sc).isNull
        case (Some(sc), Some(v)) => resRule(sc) === lit(v)
      }

      // ✅ COUNT handling:
      // - sanitize condition
      // - rewrite Table.COUNT(Col) -> __agg__Table__count__Col (wide DF contains this column now)
      val condOk: Column =
        if (fmtRule) lit(true)
        else r.conditionRaw match {
          case None => lit(true)
          case Some(c) =>
            val cleaned = ConditionSanitizer.sanitize(c)
            val rewritten = RuleDependency.rewriteCountFunctions(cleaned)
            try ConditionParser.parseToColumn(rewritten, resRule)
            catch {
              case e: Throwable =>
                throw new IllegalArgumentException(
                  s"""Condition parse failed for Dest=$destTableName.$destCol, Priority=${r.priority}, SourceTable=${r.sourceTable.getOrElse("")}
                     |Original =<<<$c>>>
                     |Cleaned  =<<<$cleaned>>>
                     |Rewritten=<<<$rewritten>>>
                     |Error    =${e.getMessage}""".stripMargin, e
                )
            }
        }

      val raw = valueSelector(r)

      val destVal: Column = {
        if (fmtRule && isFormatToken(raw)) {
          val fmt = r.conditionRaw.map(_.trim).filter(_.nonEmpty).getOrElse(
            throw new IllegalArgumentException(s"Missing format pattern in Condition for Dest=$destTableName.$destCol (DestValue=<FORMAT>).")
          )
          val srcCol = r.sourceColumn.getOrElse(
            throw new IllegalArgumentException(s"Missing SourceColumnName for Dest=$destTableName.$destCol (DestValue=<FORMAT>).")
          )
          date_format(safeToTimestamp(resRule(srcCol)), fmt)
        } else if (passThroughOnValueToken && isValueToken(raw)) {
          r.sourceColumn.map(resRule).getOrElse(lit(null))
        } else {
          raw.map(lit).getOrElse(lit(null))
        }
      }

      val effectiveCond = (srcOk && condOk) && (if (whenOnlyIfAccNull) acc.isNull else lit(true))
      when(effectiveCond, destVal).otherwise(acc)
    }
  }

  def buildForDestTable(
    destTableName: String,
    baseTable: String,
    basePk: Seq[String],
    wideDf: DataFrame,
    mappingDf: DataFrame
  ): (DataFrame, Seq[String]) = {

    val baseLower = baseTable.toLowerCase

    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTableName))
      .select("SourceTableName","SourceColumnName","SourceValue","DestColumnName","DestValue","DestValueDescription","ConversionType","Condition","Priority")
      .collect()
      .toSeq
      .filter { r =>
        val st = cleanOpt(r.getAs[Any]("SourceTableName"))
        val sc = cleanOpt(r.getAs[Any]("SourceColumnName"))
        val sv = cleanOpt(r.getAs[Any]("SourceValue"))
        val isDefault = sc.isEmpty && isDefaultToken(sv)
        st.isEmpty || st.exists(_.toLowerCase == baseLower) || isDefault
      }

    val rulesByDestCol: Map[String, Seq[MapRule]] =
      rows.map { r =>
        MapRule(
          destColumn       = cleanOpt(r.getAs[Any]("DestColumnName")).getOrElse(""),
          sourceTable      = cleanOpt(r.getAs[Any]("SourceTableName")),
          sourceColumn     = cleanOpt(r.getAs[Any]("SourceColumnName")),
          sourceValueRaw   = cleanOpt(r.getAs[Any]("SourceValue")),
          destValueRaw     = cleanOpt(r.getAs[Any]("DestValue")),
          destValueDescRaw = cleanOpt(r.getAs[Any]("DestValueDescription")),
          conversionType   = toInt(r.getAs[Any]("ConversionType"), 1),
          conditionRaw     = cleanOpt(r.getAs[Any]("Condition")),
          priority         = toInt(r.getAs[Any]("Priority"), 999999)
        )
      }
      .filter(r => r.destColumn.nonEmpty)
      .filter(r => r.conversionType == 1 || r.conversionType == 2)
      .groupBy(_.destColumn)
      .view.mapValues(_.sortBy(_.priority))
      .toMap

    var out = wideDf
    rulesByDestCol.toSeq.sortBy(_._1).foreach { case (destCol, allRulesSorted) =>
      val destTextCol = s"${destCol}Text"
      val (defaultRules, normalRules) = allRulesSorted.partition(r => r.sourceColumn.isEmpty && isDefaultToken(r.sourceValueRaw))

      val normalVal = buildExpr(destTableName, destCol, normalRules, out, whenOnlyIfAccNull = false, _.destValueRaw, passThroughOnValueToken = true)
      val defVal    = buildExpr(destTableName, destCol, defaultRules, out, whenOnlyIfAccNull = true,  _.destValueRaw, passThroughOnValueToken = true)
      val finalVal  = coalesce(normalVal, defVal)

      val normalTxt = buildExpr(destTableName, destCol, normalRules, out, whenOnlyIfAccNull = false, _.destValueDescRaw, passThroughOnValueToken = false)
      val defTxt    = buildExpr(destTableName, destCol, defaultRules, out, whenOnlyIfAccNull = true,  _.destValueDescRaw, passThroughOnValueToken = false)
      val finalTxt  = coalesce(normalTxt, defTxt)

      out = out.withColumn(destCol, finalVal).withColumn(destTextCol, finalTxt)
    }

    val mappedCols: Seq[String] = rulesByDestCol.keys.toSeq.sorted.flatMap(dc => Seq(dc, s"${dc}Text")).distinct
    val pkCols: Seq[String] = basePk.distinct.filter(out.columns.contains)

    val projected = out.select((pkCols ++ mappedCols).map(c => col(s"`$c`")): _*)
    (projected, mappedCols)
  }
}

// =================================================================================================
// 6) MAPPING AUTOMATION  (✅ COUNT deps included + pass countAggs into WideBuilder)
// =================================================================================================
object MappingAutomation {

  def defaultTableLoader(spark: SparkSession): String => DataFrame =
    (tableName: String) => spark.table(tableName)

  private def normalizeJoinEdges(joinEdgesRaw: Seq[_]): Seq[JoinEdge] = {
    def toStrSeq(x: Any): Seq[String] = x match {
      case s: Seq[_]   => s.map(_.toString)
      case a: Array[_] => a.toSeq.map(_.toString)
      case other       => Seq(other.toString)
    }
    joinEdgesRaw.map {
      case e: JoinEdge => e
      case p: Product if p.productArity >= 4 =>
        val left  = p.productElement(0).toString
        val right = p.productElement(1).toString
        val lk    = toStrSeq(p.productElement(2))
        val rk    = toStrSeq(p.productElement(3))
        val jt    = if (p.productArity >= 5) p.productElement(4).toString else "left"
        JoinEdge(left, right, lk, rk, jt)
      case other =>
        throw new IllegalArgumentException(s"Unsupported join edge type: ${other.getClass.getName}. Value=$other")
    }
  }

  private def inUniverseCI(name: String, universe: Set[String]): Boolean =
    universe.exists(_.equalsIgnoreCase(name))

  private def canonSource(name: String, universe: Set[String], label: String): String =
    universe.find(_.equalsIgnoreCase(name))
      .getOrElse(throw new IllegalArgumentException(
        s"Unknown $label '$name'. Known SOURCE tables: ${universe.toSeq.sorted.mkString(", ")}"
      ))

  private def buildGraphForTables(
    tableLoader: String => DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdges: Seq[JoinEdge],
    sourceTablesNeeded: Set[String]
  ): TableGraph = {

    val nodes: Map[String, TableNode] = sourceTablesNeeded.map { t =>
      val df = tableLoader(t)
      val pk = pkByTable(t)
      val missing = pk.filterNot(df.columns.contains)
      if (missing.nonEmpty) throw new IllegalArgumentException(s"PK columns missing in SOURCE table $t: ${missing.mkString(", ")}")
      t -> TableNode(df, pk)
    }.toMap

    val edgesFiltered = joinEdges.filter(e => nodes.contains(e.leftTable) && nodes.contains(e.rightTable))
    TableGraph(nodes, edgesFiltered)
  }

  private def addIdWithoutDroppingDuplicates(baseTable: String, graph: TableGraph, mappedDf: DataFrame): DataFrame = {
    val basePk = graph.tables(baseTable).pk.filter(mappedDf.columns.contains)
    val rnColName = "__rn__"

    val orderExpr = xxhash64(mappedDf.columns.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))): _*)

    val withRn =
      if (basePk.nonEmpty) {
        val w = Window.partitionBy(basePk.map(c => col(s"`$c`")): _*).orderBy(orderExpr)
        mappedDf.withColumn(rnColName, row_number().over(w))
      } else {
        val w = Window.orderBy(orderExpr)
        mappedDf.withColumn(rnColName, row_number().over(w))
      }

    val idParts: Seq[Column] =
      (basePk.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))) :+
        lit(baseTable) :+
        col(rnColName).cast("string"))

    val idExpr = sha2(concat_ws("||", idParts: _*), 256)

    withRn
      .withColumn("Id", idExpr)
      .drop(rnColName)
      .withColumn("_BaseSourceTable", lit(baseTable))
  }

  private def addDestTableIdAtEnd(
    destName: String,
    df: DataFrame,
    involvedSourceTables: Seq[String],
    pkByTable: Map[String, Seq[String]],
    excludeDestIdFor: Set[String],
    crashIdLogical: String = "Crash_Id"
  ): DataFrame = {

    if (excludeDestIdFor.exists(_.equalsIgnoreCase(destName))) return df

    val lowerToActual = df.columns.groupBy(_.toLowerCase).map { case (k, vs) => k -> vs.head }
    val crashCol = lowerToActual.get(crashIdLogical.toLowerCase).getOrElse(
      throw new IllegalArgumentException(
        s"Destination '$destName' DF missing Crash_Id (case-insensitive). Existing: ${df.columns.sorted.mkString(", ")}"
      )
    )

    val allPkLogical = involvedSourceTables.flatMap(t => pkByTable.getOrElse(t, Nil)).distinct
    val allPkActual  = allPkLogical.flatMap(pk => lowerToActual.get(pk.toLowerCase)).distinct
    val baseSourceActualOpt = lowerToActual.get("_basesourcetable")

    val orderCols: Seq[Column] =
      (baseSourceActualOpt.toSeq.map(c => col(s"`$c`")) ++
        allPkActual.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))))

    val w =
      if (orderCols.nonEmpty) Window.partitionBy(col(s"`$crashCol`")).orderBy(orderCols: _*)
      else Window.partitionBy(col(s"`$crashCol`")).orderBy(lit(1))

    val destIdCol = s"${destName}Id"
    val df2 = df.withColumn(destIdCol, row_number().over(w))

    val colsNoNew = df2.columns.filterNot(_.equalsIgnoreCase(destIdCol))
    df2.select((colsNoNew.map(c => col(s"`$c`")) :+ col(s"`$destIdCol`")): _*)
  }

  private def applySafeRenamesLast(df: DataFrame, renameMap: Map[String, String]): DataFrame = {
    if (renameMap == null || renameMap.isEmpty) return df
    def lowerSet(cols: Array[String]) = cols.map(_.toLowerCase).toSet

    renameMap.foldLeft(df) { case (curDf, (oldNameRaw, newNameRaw)) =>
      val oldName = Option(oldNameRaw).map(_.trim).getOrElse("")
      val newName = Option(newNameRaw).map(_.trim).getOrElse("")
      if (oldName.isEmpty || newName.isEmpty) curDf
      else {
        val cols = curDf.columns
        val colsLower = lowerSet(cols)
        val oldActualOpt = cols.find(_.equalsIgnoreCase(oldName))
        if (oldActualOpt.isDefined && !colsLower.contains(newName.toLowerCase)) curDf.withColumnRenamed(oldActualOpt.get, newName)
        else curDf
      }
    }
  }

  def buildAllDestTablesAsMap(
    spark: SparkSession,
    mappingDf: DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdgesRaw: Seq[_],
    sourceTablePriority: Map[String, Int] = Map.empty,
    destTablePriority: Map[String, Int] = Map.empty,
    excludeDestIdFor: Set[String] = Set.empty,
    renameRulesByDest: Map[String, Map[String,String]] = Map.empty,
    tableLoader: String => DataFrame = defaultTableLoader(spark)
  ): Map[String, DataFrame] = {

    val sourceUniverse = pkByTable.keySet

    val joinEdgesNorm0 = normalizeJoinEdges(joinEdgesRaw)
    val joinEdges: Seq[JoinEdge] =
      joinEdgesNorm0
        .filter(e => inUniverseCI(e.leftTable, sourceUniverse) && inUniverseCI(e.rightTable, sourceUniverse))
        .map { e =>
          val l = canonSource(e.leftTable, sourceUniverse, "SOURCE table")
          val r = canonSource(e.rightTable, sourceUniverse, "SOURCE table")
          e.copy(leftTable = l, rightTable = r)
        }

    val destTables = mappingDf.select(col("DestTableName")).distinct().collect().map(_.getString(0)).toSeq
    val destSorted = destTables.sortBy { d =>
      destTablePriority.find { case (k, _) => k.equalsIgnoreCase(d) }.map(_._2).getOrElse(999999) -> d.toLowerCase
    }

    val outPairs: Seq[(String, DataFrame)] = destSorted.map { dest =>

      // Normal deps
      val neededTablesRaw = RuleDependency.requiredTableColsForDest(mappingDf, dest).keySet

      // ✅ COUNT deps (tables referenced by Table.COUNT(Column))
      val countAggsForDestRaw = RuleDependency.requiredCountAggsForDest(mappingDf, dest)

      // Source tables from mapping rows
      val srcFromMappingRaw = mappingDf
        .filter(col("DestTableName") === lit(dest))
        .select(col("SourceTableName"))
        .distinct()
        .collect()
        .map(_.getString(0))
        .filter(_ != null)
        .toSet

      // Combine all required source tables: normal + mapping + COUNT tables
      val involvedSourceTables: Seq[String] =
        (neededTablesRaw ++ srcFromMappingRaw ++ countAggsForDestRaw.keySet)
          .filter(_ != null)
          .filter(t => inUniverseCI(t, sourceUniverse))
          .map(t => canonSource(t, sourceUniverse, "SOURCE table"))
          .toSeq
          .distinct
          .sortBy(_.toLowerCase)

      if (involvedSourceTables.isEmpty) {
        throw new IllegalArgumentException(
          s"DestTable='$dest' has no valid SOURCE tables in mapping/conditions. Known SOURCE tables: ${sourceUniverse.toSeq.sorted.mkString(", ")}"
        )
      }

      val graph = buildGraphForTables(tableLoader, pkByTable, joinEdges, involvedSourceTables.toSet)

      val baseTablesOrdered: Seq[String] =
        involvedSourceTables.sortBy { t =>
          sourceTablePriority.find { case (k, _) => k.equalsIgnoreCase(t) }.map(_._2).getOrElse(999999) -> t.toLowerCase
        }

      val perBase: Seq[DataFrame] = baseTablesOrdered.map { baseTable =>
        val (wide, baseCanon, _) =
          WideBuilder.buildWideDf(
            graph = graph,
            mappingDf = mappingDf,
            destTable = dest,
            baseTable = baseTable,
            keepAllPk = true,
            countAggsForDest = countAggsForDestRaw // ✅ provide COUNT aggregates for this dest
          )

        val (mappedOnly, _) = DestTableBuilder.buildForDestTable(dest, baseCanon, graph.tables(baseCanon).pk, wide, mappingDf)
        addIdWithoutDroppingDuplicates(baseCanon, graph, mappedOnly)
      }

      val unioned =
        if (perBase.size == 1) perBase.head
        else perBase.reduce(_.unionByName(_, allowMissingColumns = true))

      val withDestId = addDestTableIdAtEnd(
        destName = dest,
        df = unioned,
        involvedSourceTables = involvedSourceTables,
        pkByTable = pkByTable,
        excludeDestIdFor = excludeDestIdFor,
        crashIdLogical = "Crash_Id"
      )

      val renameMapForDest: Map[String,String] =
        renameRulesByDest.find { case (k, _) => k.equalsIgnoreCase(dest) }.map(_._2).getOrElse(Map.empty)

      val finalDf = applySafeRenamesLast(withDestId, renameMapForDest)
      dest -> finalDf
    }

    outPairs.toMap
  }
}

// =================================================================================================
// USAGE EXAMPLE
// -------------------------------------------------------------------------------------------------
// val renameRulesByDest: Map[String, Map[String,String]] = Map(
//   "Person" -> Map("Unit_id" -> "Vehicle_id")
// )
//
// val outMap = MappingAutomation.buildAllDestTablesAsMap(
//   spark = spark,
//   mappingDf = mappingDf,
//   pkByTable = pkByTable,
//   joinEdgesRaw = joinEdges,
//   sourceTablePriority = sourceTablePriority,
//   excludeDestIdFor = Set("Address"),
//   renameRulesByDest = renameRulesByDest
// )
//
// display(outMap("Vehicle"))
// =================================================================================================
