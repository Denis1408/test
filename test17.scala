// =================================================================================================
// FINAL ONE-CELL SCRIPT (ALL FIXES + TYPE1 <FORMAT> + SAFE STRING DATE/TIME PARSING)
// -------------------------------------------------------------------------------------------------
// ✅ Mapping-driven load from MANY source tables -> MANY destination tables
// ✅ Returns: Map[DestTableName -> DataFrame]
// ✅ JoinEdge mismatch fix: joinEdgesRaw: Seq[_] normalized -> JoinEdge
// ✅ MULTI-SOURCE DEST: process EACH involved source table as base, UNION ALL rows (keeps duplicates)
// ✅ NO CROSS-ENRICH: when base is X, apply ONLY rules with SourceTableName==X (plus SourceTableName NULL + <DEFAULT>)
// ✅ Condition parser supports: IS NULL / IS NOT NULL / = NULL / != NULL / IN / NOT IN / AND / OR / && / ||
// ✅ Type 2 wildcards:
//      - SourceValue == "<NULL>" => wildcard (any value)
//      - SourceValue == NULL     => wildcard (any value)
// ✅ <VALUE> pass-through, <DEFAULT> fallback
// ✅ DestValueDescription -> "<DestColumnName>Text"
// ✅ Adds Id (does NOT drop duplicates): Id = sha2( PKs + baseTable + row_number() )
// ✅ Case-insensitive table + column handling
// ✅ Type 1 formatting:
//      - If ConversionType=1 AND SourceValue=<VALUE> AND DestValue=<FORMAT>
//        then DestCol = date_format( safeToTimestamp(sourceCol), pattern )
//        where pattern = Condition column (e.g., yyyy-MM-dd, HHmm, HHmmss, etc.)
// ✅ SAFE parsing: uses try_to_timestamp with multiple common patterns (prevents CAST_INVALID_INPUT)
// =================================================================================================

import org.apache.spark.sql.{DataFrame, Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.util.Try
import scala.util.matching.Regex

// ------------------------------
// Models
// ------------------------------
case class JoinEdge(leftTable: String, rightTable: String, leftKeys: Seq[String], rightKeys: Seq[String], joinType: String = "left")
case class TableNode(df: DataFrame, pk: Seq[String])
case class TableGraph(tables: Map[String, TableNode], edges: Seq[JoinEdge])

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

// ------------------------------
// Condition Sanitizer
// ------------------------------
object ConditionSanitizer {
  def sanitize(raw: String): String = {
    if (raw == null) return null
    var s = raw.trim
    s = s
      .replace('\u201C', '"').replace('\u201D', '"')
      .replace('\u2018', '\'').replace('\u2019', '\'')
    while (s.endsWith(";")) s = s.dropRight(1).trim

    val trailingBad = Seq("AND", "OR", "&&", "||", "(")
    var changed = true
    while (changed) {
      changed = false
      val up = s.toUpperCase
      trailingBad.find(tok => up.endsWith(tok)) match {
        case Some(tok) => s = s.dropRight(tok.length).trim; changed = true
        case None =>
      }
    }

    val open = s.count(_ == '(')
    val close = s.count(_ == ')')
    if (open > close) s = s + (")" * (open - close))
    s
  }
}

// ------------------------------
// Condition Parser (supports "= NULL" and "!= NULL")
// ------------------------------
object ConditionParser {

  sealed trait Tok { def s: String }
  case class ID(s: String) extends Tok
  case class STR(s: String) extends Tok
  case class NUM(s: String) extends Tok
  case class OP(s: String) extends Tok
  case class LP(s: String = "(") extends Tok
  case class RP(s: String = ")") extends Tok
  case class COMMA(s: String = ",") extends Tok

  private val twoCharOps = Set("==", "!=", "<>", "&&", "||", "<=", ">=")
  private val oneCharOps = Set("=", "<", ">", "!")
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
    def isIdPart(ch: Char) = ch.isLetterOrDigit || ch == '_' || ch == '.'

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
          val up = raw.toUpperCase
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
      case _ => parseComparison()
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
        case _ =>
          leftTerm
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
      case Some(ID(name)) => next(); colResolver(name)
      case Some(STR(v))   => next(); lit(v)
      case Some(NUM(v))   => next(); Try(v.toLong).map(lit).getOrElse(Try(v.toDouble).map(lit).getOrElse(lit(v)))
      case Some(OP("NULL")) => next(); lit(null)
      case other => throw new IllegalArgumentException(s"Expected term but found: ${other.map(_.s)}")
    }

    private def parseLiteralAny(): Any = peek match {
      case Some(STR(v)) => next(); v
      case Some(NUM(v)) => next(); Try(v.toLong).getOrElse(Try(v.toDouble).getOrElse(v))
      case Some(OP("NULL")) => next(); null
      case other => throw new IllegalArgumentException(s"Expected literal but found: ${other.map(_.s)}")
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

// ------------------------------
// Dependency extraction from mapping + conditions (tables in CAPS allowed)
// ------------------------------
object RuleDependency {
  private val tableColRx: Regex = """([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)""".r
  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

  def requiredTableColsForDest(mappingDf: DataFrame, destTable: String): Map[String, Set[String]] = {
    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTable))
      .select("SourceTableName","SourceColumnName","Condition")
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
}

// ------------------------------
// Wide DF Builder
// ------------------------------
object WideBuilder {
  private def canonicalTableName(graph: TableGraph, t: String): String =
    graph.tables.keys.find(_.equalsIgnoreCase(t))
      .getOrElse(throw new IllegalArgumentException(s"Unknown table '$t'. Known: ${graph.tables.keys.toSeq.sorted.mkString(", ")}"))

  private def selectWithAliases(tableName: String, df: DataFrame, cols: Set[String], keepKeys: Seq[String], keyPrefix: Option[String]): DataFrame = {
    val keys = keepKeys.distinct.filter(df.columns.contains)
    val dataCols = cols.filter(df.columns.contains).toSeq.distinct.sorted

    val keySelect: Seq[Column] = keyPrefix match {
      case None => keys.map(col)
      case Some(pfx) => keys.map(k => col(k).as(s"${pfx}${k}"))
    }
    val dataSelect: Seq[Column] = dataCols.map(c => col(c).as(s"$tableName.$c"))
    val sel = keySelect ++ dataSelect
    if (sel.isEmpty) df else df.select(sel: _*)
  }

  private def buildAdj(edges: Seq[JoinEdge]): Map[String, Seq[JoinEdge]] =
    edges.flatMap(e => Seq(e.leftTable -> e, e.rightTable -> e)).groupBy(_._1).view.mapValues(_.map(_._2)).toMap

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
        if (!seen.contains(nxt)) { seen += nxt; prevEdge(nxt) = e; prevNode(nxt) = cur; q.enqueue(nxt) }
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

  def buildWideDf(graph: TableGraph, mappingDf: DataFrame, destTable: String, baseTable: String, keepAllPk: Boolean): (DataFrame, String, Set[String]) = {
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

          val joinCond = lk.zip(rk).map { case (lKey, rKey) => wide.col(lKey) === rightDf.col(s"${pfx}${rKey}") }.reduce(_ && _)
          val joinedDf = wide.join(rightDf, joinCond, e.joinType)

          wide =
            if (keepAllPk) joinedDf
            else joinedDf.drop(rk.map(rKey => s"${pfx}${rKey}"): _*)

          joined += nextTable
        }
        curTable = nextTable
      }
    }
    (wide, base, neededTables.toSet)
  }
}

// ------------------------------
// DestTableBuilder (Type2 NULL wildcard + Type1 <FORMAT> + SAFE string parsing)
// ------------------------------
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

  // ✅ Safe parsing for common string date/time formats (returns null if cannot parse)
  // Add more formats here if your data has more patterns.
  private def safeToTimestamp(c: Column): Column = {
    val s = c.cast("string")
    coalesce(
      // date-only
      try_to_timestamp(s, "MM/dd/yyyy"),
      try_to_timestamp(s, "M/d/yyyy"),
      try_to_timestamp(s, "MM-dd-yyyy"),
      try_to_timestamp(s, "M-d-yyyy"),
      try_to_timestamp(s, "yyyy-MM-dd"),
      try_to_timestamp(s, "yyyy/MM/dd"),
      try_to_timestamp(s, "yyyyMMdd"),
      // datetime
      try_to_timestamp(s, "MM/dd/yyyy HH:mm:ss"),
      try_to_timestamp(s, "MM/dd/yyyy H:mm:ss"),
      try_to_timestamp(s, "MM/dd/yyyy HH:mm"),
      try_to_timestamp(s, "MM/dd/yyyy H:mm"),
      // time-only (attach dummy date)
      try_to_timestamp(concat(lit("1970-01-01 "), s), "yyyy-MM-dd HH:mm:ss"),
      try_to_timestamp(concat(lit("1970-01-01 "), s), "yyyy-MM-dd H:mm:ss"),
      try_to_timestamp(concat(lit("1970-01-01 "), s), "yyyy-MM-dd HH:mm"),
      try_to_timestamp(concat(lit("1970-01-01 "), s), "yyyy-MM-dd H:mm"),
      // last resort
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
            case 0 => throw new IllegalArgumentException(s"Cannot resolve column '$name0' (case-insensitive). Example: ${cols.take(50).mkString(", ")}")
            case _ =>
              val preferred = defaultTable.flatMap(t => lowerToActual.get((t + "." + tail).toLowerCase))
              preferred.map(litCol).getOrElse(throw new IllegalArgumentException(s"Ambiguous column '$tail' matches: ${dottedMatches.mkString(", ")}"))
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

      // format-rule detected from RULE (not from selector)
      val fmtRule: Boolean =
        r.conversionType == 1 && isValueToken(r.sourceValueRaw) && isFormatToken(r.destValueRaw)

      val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
        case (None, _) => lit(true)
        case (Some(_), sv) if isValueToken(sv) => lit(true)
        case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true) // <NULL> wildcard
        case (Some(_), None) if r.conversionType == 2 => lit(true)          // Type2 NULL => wildcard
        case (Some(sc), None) => resRule(sc).isNull                          // Type1 NULL => isNull
        case (Some(sc), Some(v)) => resRule(sc) === lit(v)
      }

      // if format-rule -> Condition is format pattern => never parse as boolean
      val condOk: Column =
        if (fmtRule) lit(true)
        else r.conditionRaw match {
          case None => lit(true)
          case Some(c) =>
            val cleaned = ConditionSanitizer.sanitize(c)
            try ConditionParser.parseToColumn(cleaned, resRule)
            catch {
              case e: Throwable =>
                throw new IllegalArgumentException(
                  s"""Condition parse failed for Dest=$destTableName.$destCol, Priority=${r.priority}, SourceTable=${r.sourceTable.getOrElse("")}
                     |Original=<<<$c>>>
                     |Cleaned =<<<$cleaned>>>
                     |Error   =${e.getMessage}""".stripMargin, e
                )
            }
        }

      val raw = valueSelector(r)

      val destVal: Column = {
        // Apply formatting ONLY when building VALUE column (selector returns DestValueRaw == <FORMAT>)
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

  def buildForDestTable(destTableName: String, baseTable: String, basePk: Seq[String], wideDf: DataFrame, mappingDf: DataFrame): (DataFrame, Seq[String]) = {

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

      // Text: not formatted (description is literal). Format-rule still skips boolean parsing safely.
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
// AUTOMATION
// =================================================================================================
object MappingAutomation {

  def defaultTableLoader(spark: SparkSession): String => DataFrame =
    (tableName: String) => spark.table(tableName)

  private def normalizeJoinEdges(joinEdgesRaw: Seq[_]): Seq[JoinEdge] = {
    def toStrSeq(x: Any): Seq[String] = x match {
      case s: Seq[_] => s.map(_.toString)
      case a: Array[_] => a.toSeq.map(_.toString)
      case other => Seq(other.toString)
    }

    joinEdgesRaw.map {
      case e: JoinEdge => e
      case p: Product if p.productArity >= 4 =>
        val left  = p.productElement(0).toString
        val right = p.productElement(1).toString
        val lk = toStrSeq(p.productElement(2))
        val rk = toStrSeq(p.productElement(3))
        val jt = if (p.productArity >= 5) p.productElement(4).toString else "left"
        JoinEdge(left, right, lk, rk, jt)
      case other =>
        throw new IllegalArgumentException(s"Unsupported join edge type: ${other.getClass.getName}. Value=$other")
    }
  }

  private def inUniverseCI(name: String, universe: Set[String]): Boolean =
    universe.exists(_.equalsIgnoreCase(name))

  private def canonSource(name: String, universe: Set[String], label: String): String =
    universe.find(_.equalsIgnoreCase(name))
      .getOrElse(throw new IllegalArgumentException(s"Unknown $label '$name'. Known SOURCE tables: ${universe.toSeq.sorted.mkString(", ")}"))

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

  private def addIdWithoutDroppingDuplicates(
    baseTable: String,
    graph: TableGraph,
    mappedDf: DataFrame
  ): DataFrame = {

    val basePk = graph.tables(baseTable).pk.filter(mappedDf.columns.contains)
    val rnColName = "__rn__"

    val orderExpr =
      xxhash64(mappedDf.columns.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))): _*)

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

  def buildAllDestTablesAsMap(
    spark: SparkSession,
    mappingDf: DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdgesRaw: Seq[_],
    sourceTablePriority: Map[String, Int] = Map.empty,
    destTablePriority: Map[String, Int] = Map.empty,
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

      val neededTablesRaw = RuleDependency.requiredTableColsForDest(mappingDf, dest).keySet

      val srcFromMappingRaw = mappingDf
        .filter(col("DestTableName") === lit(dest))
        .select(col("SourceTableName"))
        .distinct()
        .collect()
        .map(_.getString(0))
        .filter(_ != null)
        .toSet

      val involvedSourceTables: Set[String] =
        (neededTablesRaw ++ srcFromMappingRaw)
          .filter(_ != null)
          .filter(t => inUniverseCI(t, sourceUniverse))
          .map(t => canonSource(t, sourceUniverse, "SOURCE table"))

      if (involvedSourceTables.isEmpty) {
        throw new IllegalArgumentException(
          s"DestTable='$dest' has no valid SOURCE tables in mapping/conditions. Known SOURCE tables: ${sourceUniverse.toSeq.sorted.mkString(", ")}"
        )
      }

      val graph = buildGraphForTables(tableLoader, pkByTable, joinEdges, involvedSourceTables)

      val baseTablesOrdered: Seq[String] =
        involvedSourceTables.toSeq.sortBy { t =>
          sourceTablePriority.find { case (k, _) => k.equalsIgnoreCase(t) }.map(_._2).getOrElse(999999) -> t.toLowerCase
        }

      val perBase: Seq[DataFrame] = baseTablesOrdered.map { baseTable =>
        val (wide, baseCanon, _) = WideBuilder.buildWideDf(graph, mappingDf, dest, baseTable, keepAllPk = true)
        val (mappedOnly, _) = DestTableBuilder.buildForDestTable(dest, baseCanon, graph.tables(baseCanon).pk, wide, mappingDf)
        addIdWithoutDroppingDuplicates(baseCanon, graph, mappedOnly)
      }

      val finalDf =
        if (perBase.size == 1) perBase.head
        else perBase.reduce(_.unionByName(_, allowMissingColumns = true))

      dest -> finalDf
    }

    outPairs.toMap
  }
}

// =================================================================================================
// USAGE
// -------------------------------------------------------------------------------------------------
// val tableLoader: String => DataFrame = (t: String) => spark.table(s"catalog.schema.$t")
//
// val outMap = MappingAutomation.buildAllDestTablesAsMap(
//   spark = spark,
//   mappingDf = mappingDf,
//   pkByTable = pkByTable,
//   joinEdgesRaw = joinEdges,
//   sourceTablePriority = sourceTablePriority,
//   tableLoader = tableLoader
// )
//
// display(outMap("Crash"))
// =================================================================================================
