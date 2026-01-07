// =================================================================================================
// FINAL ONE-CELL SCRIPT (Spark / Databricks Scala) — ALL FEATURES MERGED + NULL FIX
// -------------------------------------------------------------------------------------------------
// What this does:
//   Build destination DataFrames from multiple source tables using a Mapping table.
//
// Mapping table columns expected:
//   SourceTableName, SourceColumnName, SourceValue,
//   DestTableName, DestColumnName, DestValue, DestValueDescription,
//   ConversionType, Condition, Priority
//
// Supported mapping behavior:
//   ✅ Auto-build a "wide" DataFrame for a DestTableName by reading dependencies from mapping rules
//   ✅ Auto-join source tables using a TableGraph (multi-hop supported)
//   ✅ Apply rules per destination column using Priority (1 is highest)
//   ✅ Conditions parsing supports:
//        AND/OR/&&/||, parentheses,
//        = == != <>, < > <= >=,
//        IN / NOT IN,
//        IS NULL / IS NOT NULL,
//        ALSO "= NULL" and "!= NULL" / "<> NULL"   (NEW FIX)
//   ✅ Tokens:
//        - SourceValue SQL NULL     => match null source values
//        - SourceValue "<VALUE>"    => wildcard (any source value)
//        - SourceValue "<NULL>"     => wildcard for ConversionType=2 (any source value)
//        - DestValue   "<VALUE>"    => pass-through source column value
//        - SourceColumnName NULL + SourceValue "<DEFAULT>" => DEFAULT fallback rule (applies only if no other rule matched)
//   ✅ DestValueDescription -> creates column DestColumnName + "Text"
//   ✅ Final output for each destination table contains ONLY:
//        base PK columns + mapped DestColumnName columns + DestColumnNameText columns
//
// IMPORTANT:
//   Dotted columns like "Unit.DriverAge" are literal column names (not structs). We always reference with backticks.
// =================================================================================================

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
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
  sourceValueRaw: Option[String],        // None => SQL NULL
  destValueRaw: Option[String],          // None => SQL NULL
  destValueDescRaw: Option[String],      // None => SQL NULL
  conversionType: Int,
  conditionRaw: Option[String],
  priority: Int
)

// ------------------------------
// Condition Sanitizer (makes mapping Condition strings parseable)
// ------------------------------
object ConditionSanitizer {
  def sanitize(raw: String): String = {
    if (raw == null) return null
    var s = raw.trim

    // normalize “smart quotes”
    s = s
      .replace('\u201C', '"').replace('\u201D', '"')
      .replace('\u2018', '\'').replace('\u2019', '\'')

    while (s.endsWith(";")) s = s.dropRight(1).trim

    // trim dangling operators / dangling '(' at the end
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

    // balance parentheses
    val open = s.count(_ == '(')
    val close = s.count(_ == ')')
    if (open > close) s = s + (")" * (open - close))

    s
  }
}

// ------------------------------
// Condition Parser: SQL-like text -> Spark Column
// Supports: AND/OR/&&/||, (), = == != <>, < > <= >=, IN/NOT IN, IS NULL/IS NOT NULL,
//           PLUS "= NULL" and "!= NULL" / "<> NULL" (NEW FIX)
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
          if (i + 1 < n && twoCharOps.contains(input.substring(i, i + 2))) {
            out += OP(input.substring(i, i + 2)); i += 2
          } else if (oneCharOps.contains(input.charAt(i).toString)) {
            out += OP(input.charAt(i).toString); i += 1
          } else {
            throw new IllegalArgumentException(s"Unexpected character '${input.charAt(i)}' at position $i in: $input0")
          }
      }
    }
    out.result()
  }

  private final class Parser(tokens: Vector[Tok], colResolver: String => Column) {
    private var pos = 0
    private def peek: Option[Tok] = if (pos < tokens.length) Some(tokens(pos)) else None
    private def peek2: Option[Tok] = if (pos + 1 < tokens.length) Some(tokens(pos + 1)) else None
    private def next(): Tok = { val t = tokens(pos); pos += 1; t }

    def expectEnd(): Unit = {
      if (pos != tokens.length) throw new IllegalArgumentException(s"Unexpected token at end: ${tokens(pos).s}")
    }

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
        // NEW FIX: handle "= NULL" and "!= NULL" / "<> NULL" without parsing NULL as a term
        case Some(OP("=" | "==")) if peek2.contains(OP("NULL")) =>
          next(); next(); leftTerm.isNull

        case Some(OP("!=" | "<>")) if peek2.contains(OP("NULL")) =>
          next(); next(); leftTerm.isNotNull

        // regular comparisons
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
          next(); expectOp("IN")
          val values = parseInList()
          !leftTerm.isin(values: _*)

        case Some(OP("IN")) =>
          next()
          val values = parseInList()
          leftTerm.isin(values: _*)

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
      // Allow bare NULL term in rare cases (but main "=NULL" logic handled above)
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
// Dependency extraction: which columns/tables are needed for a DestTableName
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
// Wide DF Builder: auto-select + auto-join (multi-hop) + safe join keys
// ------------------------------
object WideBuilder {

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

  def buildWideDf(graph: TableGraph, mappingDf: DataFrame, destTable: String, baseTable: String): DataFrame = {
    val req = RuleDependency.requiredTableColsForDest(mappingDf, destTable)
    val neededTables = (req.keySet + baseTable).toSeq.distinct

    val baseNode = graph.tables.getOrElse(baseTable, throw new IllegalArgumentException(s"Missing base table: $baseTable"))
    var wide = selectWithAliases(baseTable, baseNode.df, req.getOrElse(baseTable, Set.empty), baseNode.pk, None)

    val joined = scala.collection.mutable.Set[String](baseTable)

    def getTableDf(t: String): (DataFrame, String) = {
      val node = graph.tables.getOrElse(t, throw new IllegalArgumentException(s"Missing table: $t"))
      val pfx  = s"__k__${t}__"
      val dfSel = selectWithAliases(t, node.df, req.getOrElse(t, Set.empty), node.pk, Some(pfx))
      (dfSel, pfx)
    }

    neededTables.filterNot(_ == baseTable).foreach { target =>
      val pathEdges = findPath(graph.edges, baseTable, target)
      var curTable = baseTable

      pathEdges.foreach { e =>
        val nextTable =
          if (e.leftTable == curTable) e.rightTable
          else if (e.rightTable == curTable) e.leftTable
          else throw new IllegalArgumentException(s"Non-contiguous path at $curTable for edge $e")

        if (!joined.contains(nextTable)) {
          val (lk, rk) = edgeKeys(curTable, nextTable, e)
          val (rightDf, pfx) = getTableDf(nextTable)

          val joinCond = lk.zip(rk).map { case (lKey, rKey) => wide.col(lKey) === rightDf.col(s"${pfx}${rKey}") }.reduce(_ && _)
          val joinedDf = wide.join(rightDf, joinCond, e.joinType)

          wide = joinedDf.drop(rk.map(rKey => s"${pfx}${rKey}"): _*)
          joined += nextTable
        }
        curTable = nextTable
      }
    }
    wide
  }
}

// ------------------------------
// DestTableBuilder: apply mapping rules + default fallback + Text columns + output projection
// ------------------------------
object DestTableBuilder {

  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

  private def toInt(v: Any, default: Int): Int =
    Option(v).flatMap(x => Try(x.toString.trim.toInt).toOption).getOrElse(default)

  // token helpers
  private def isValueToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<VALUE>") || v.equalsIgnoreCase("&lt;VALUE&gt;"))

  private def isNullTextToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<NULL>") || v.equalsIgnoreCase("&lt;NULL&gt;"))

  private def isDefaultToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<DEFAULT>") || v.equalsIgnoreCase("&lt;DEFAULT&gt;"))

  // Resolver: dot-safe (backticks) + unqualified resolution + ambiguity defaulting by SourceTableName
  private def resolver(wideDf: DataFrame, defaultTable: Option[String]): String => Column = {
    (name0: String) => {
      val name = name0.trim
      val tail = name.split('.').last
      def litCol(colName: String): Column = col(s"`$colName`") // dot-safe

      if (wideDf.columns.contains(name)) {
        litCol(name)
      } else if (wideDf.columns.contains(tail)) {
        litCol(tail)
      } else {
        val dottedMatches = wideDf.columns.filter(c => c.endsWith("." + tail))
        dottedMatches.length match {
          case 1 => litCol(dottedMatches.head)
          case 0 =>
            throw new IllegalArgumentException(
              s"Cannot resolve column '$name0'. Try qualifying it (e.g. 'Unit.$tail'). Available: ${wideDf.columns.take(50).mkString(", ")}"
            )
          case _ =>
            val preferred = defaultTable.flatMap { t =>
              val cand = s"$t.$tail"
              if (wideDf.columns.contains(cand)) Some(cand) else None
            }
            preferred match {
              case Some(c) => litCol(c)
              case None =>
                throw new IllegalArgumentException(
                  s"Ambiguous column '$tail' matches: ${dottedMatches.mkString(", ")}. Qualify it in Condition."
                )
            }
        }
      }
    }
  }

  // Build expression from a list of rules.
  // If whenOnlyIfAccNull=true, rule applies only if accumulated value is still NULL (for DEFAULT fallback).
  private def buildExpr(
    destTableName: String,
    destCol: String,
    rules: Seq[MapRule],
    outDf: DataFrame,
    whenOnlyIfAccNull: Boolean,
    valueSelector: MapRule => Option[String],     // DestValueRaw or DestValueDescRaw
    passThroughOnValueToken: Boolean             // only for DestValueRaw (main value column)
  ): Column = {
    rules.foldLeft(lit(null).cast("string")) { case (acc, r) =>
      val resRule: String => Column = resolver(outDf, r.sourceTable)

      val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
        case (None, _) => lit(true) // DEFAULT rules have no sourceColumn
        case (Some(_), sv) if isValueToken(sv) => lit(true)
        case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true) // wildcard for type 2
        case (Some(sc), None) => resRule(sc).isNull                           // SQL NULL
        case (Some(sc), Some(v)) => resRule(sc) === lit(v)                    // normal equality
      }

      val condOk: Column = r.conditionRaw match {
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

      val finalCond = srcOk && condOk

      val raw = valueSelector(r)
      val destVal: Column =
        if (passThroughOnValueToken && isValueToken(raw)) r.sourceColumn.map(resRule).getOrElse(lit(null))
        else raw.map(lit).getOrElse(lit(null))

      val effectiveCond = if (whenOnlyIfAccNull) (acc.isNull && finalCond) else finalCond
      when(effectiveCond, destVal).otherwise(acc)
    }
  }

  // Build mapped DF for one destination table; returns ONLY base PK + mapped cols (+Text)
  def buildForDestTable(destTableName: String, basePk: Seq[String], wideDf: DataFrame, mappingDf: DataFrame): DataFrame = {

    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTableName))
      .select(
        col("SourceTableName"),
        col("SourceColumnName"),
        col("SourceValue"),
        col("DestColumnName"),
        col("DestValue"),
        col("DestValueDescription"),
        col("ConversionType"),
        col("Condition"),
        col("Priority")
      )
      .collect()
      .toSeq

    val rulesByDestCol: Map[String, Seq[MapRule]] =
      rows
        .map { r =>
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

      // DEFAULT fallback rules:
      //   SourceColumnName is NULL AND SourceValue="<DEFAULT>"
      val (defaultRules, normalRules) =
        allRulesSorted.partition(r => r.sourceColumn.isEmpty && isDefaultToken(r.sourceValueRaw))

      // value column
      val normalVal = buildExpr(destTableName, destCol, normalRules, out, whenOnlyIfAccNull = false, _.destValueRaw, passThroughOnValueToken = true)
      val defVal    = buildExpr(destTableName, destCol, defaultRules, out, whenOnlyIfAccNull = true,  _.destValueRaw, passThroughOnValueToken = true)
      val finalVal  = coalesce(normalVal, defVal)

      // text column (DestValueDescription)
      val normalTxt = buildExpr(destTableName, destCol, normalRules, out, whenOnlyIfAccNull = false, _.destValueDescRaw, passThroughOnValueToken = false)
      val defTxt    = buildExpr(destTableName, destCol, defaultRules, out, whenOnlyIfAccNull = true,  _.destValueDescRaw, passThroughOnValueToken = false)
      val finalTxt  = coalesce(normalTxt, defTxt)

      out =
        if (out.columns.contains(destCol)) out.withColumn(destCol, coalesce(finalVal, col(s"`$destCol`")))
        else out.withColumn(destCol, finalVal)

      out =
        if (out.columns.contains(destTextCol)) out.withColumn(destTextCol, coalesce(finalTxt, col(s"`$destTextCol`")))
        else out.withColumn(destTextCol, finalTxt)
    }

    // Final projection: ONLY PK + mapped columns (+Text)
    val mappedCols: Seq[String] =
      rulesByDestCol.keys.toSeq.sorted.flatMap(dc => Seq(dc, s"${dc}Text")).distinct

    val pkCols: Seq[String] = basePk.distinct.filter(out.columns.contains)

    out.select((pkCols ++ mappedCols).map(c => col(s"`$c`")): _*)
  }
}

// ------------------------------
// Public helper: build destination output in one call
// ------------------------------
def buildDestTable(destTableName: String, baseTable: String, graph: TableGraph, mappingDf: DataFrame): DataFrame = {
  val baseNode = graph.tables.getOrElse(baseTable, throw new IllegalArgumentException(s"Missing base table: $baseTable"))
  val wide = WideBuilder.buildWideDf(graph, mappingDf, destTableName, baseTable)
  DestTableBuilder.buildForDestTable(destTableName, baseNode.pk, wide, mappingDf)
}

// =================================================================================================
// USAGE EXAMPLE (adapt to your real schema)
// -------------------------------------------------------------------------------------------------
// val dfUnit: DataFrame = ...
// val dfPersonInj: DataFrame = ...
// val dfCrash: DataFrame = ...
// val mappingDf: DataFrame = ... // must include DestValueDescription
//
// val graph = TableGraph(
//   tables = Map(
//     "Crash" -> TableNode(dfCrash, pk = Seq("Crash_Id")),
//     "Unit"  -> TableNode(dfUnit,  pk = Seq("Crash_Id","Unit_Id")),
//     "Person_Injured" -> TableNode(dfPersonInj, pk = Seq("Crash_Id","Unit_Id","Person_Id"))
//   ),
//   edges = Seq(
//     JoinEdge("Crash", "Unit", Seq("Crash_Id"), Seq("Crash_Id"), "left"),
//     JoinEdge("Unit", "Person_Injured", Seq("Crash_Id","Unit_Id"), Seq("Crash_Id","Unit_Id"), "left")
//   )
// )
//
// val personOut = buildDestTable(destTableName = "Person", baseTable = "Person_Injured", graph, mappingDf)
// personOut.show(false)
// =================================================================================================
