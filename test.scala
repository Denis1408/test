// =================================================================================================
// FINAL ONE-CELL SOLUTION (Spark / Databricks Scala) - UPDATED
// -------------------------------------------------------------------------------------------------
// NEW REQUIREMENTS ADDED (per your last message):
//   1) ConversionType=2: if SourceValue equals the TEXT token "<NULL>", then source value can be ANY
//      (i.e., do NOT filter by SourceValue for that rule). This is different from real SQL NULL.
//   2) Add DestValueDescription mapping into a NEW column:
//        NewColumnName = DestColumnName + "Text"
//      Example: DestColumnName="InjuryStatus" => new column "InjuryStatusText"
//   3) Final result dataframes must contain ONLY:
//        - Primary keys (from base table PK in TableGraph)
//        - Mapped columns (DestColumnName)
//        - Mapped text columns (DestColumnName + "Text")
//
// STILL SUPPORTED (from previous iterations):
//   ✅ Auto-build wide DF from mapping dependencies (Source + Condition refs)
//   ✅ Auto-join source tables using join graph (multi-hop supported)
//   ✅ Dot-column names are kept as literal via backticks: col("`Unit.DriverAge`")
//   ✅ Condition parser: AND/OR/&&/||, (), = == != <>, < > <= >=, IN/NOT IN, IS NULL/IS NOT NULL
//   ✅ Tokens:
//        SourceValue NULL (actual NULL) => match null source values
//        SourceValue <VALUE>            => ANY source value
//        DestValue   <VALUE>            => pass-through source column value
//   ✅ Priority (1 is highest). NULL priority -> lowest.
//
// Mapping table columns expected:
//   SourceTableName, SourceColumnName, SourceValue,
//   DestTableName, DestColumnName, DestValue, DestValueDescription,
//   ConversionType, Condition, Priority
// =================================================================================================

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import scala.util.Try
import scala.util.matching.Regex

// ------------------------------
// Models
// ------------------------------
case class JoinEdge(
  leftTable: String,
  rightTable: String,
  leftKeys: Seq[String],
  rightKeys: Seq[String],
  joinType: String = "left"
)
case class TableNode(df: DataFrame, pk: Seq[String])
case class TableGraph(tables: Map[String, TableNode], edges: Seq[JoinEdge])

case class MapRule(
  destColumn: String,
  sourceTable: Option[String],
  sourceColumn: Option[String],
  sourceValueRaw: Option[String],        // None => SourceValue is SQL NULL
  destValueRaw: Option[String],          // None => DestValue is SQL NULL
  destValueDescRaw: Option[String],      // None => DestValueDescription is SQL NULL
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

    // normalize smart quotes -> plain quotes
    s = s
      .replace('\u201C', '"').replace('\u201D', '"')
      .replace('\u2018', '\'').replace('\u2019', '\'')

    while (s.endsWith(";")) s = s.dropRight(1).trim

    // trim dangling operators / dangling '(' at end
    val trailingBad = Seq("AND", "OR", "&&", "||", "(")
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

    // balance parentheses
    val open = s.count(_ == '(')
    val close = s.count(_ == ')')
    if (open > close) s = s + (")" * (open - close))

    s
  }
}

// ------------------------------
// Condition Parser: SQL-like -> Spark Column
// Supports: AND/OR/&&/||, (), = == != <>, < > <= >=, IN/NOT IN, IS NULL/IS NOT NULL
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
          val quote = input.charAt(i)
          i += 1
          val start = i
          while (i < n && input.charAt(i) != quote) i += 1
          out += STR(input.substring(start, i))
          if (i < n && input.charAt(i) == quote) i += 1

        case ch if ch.isDigit =>
          val start = i
          while (i < n && (input.charAt(i).isDigit || input.charAt(i) == '.')) i += 1
          out += NUM(input.substring(start, i))

        case ch if isIdStart(ch) =>
          val start = i
          i += 1
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
    private def next(): Tok = { val t = tokens(pos); pos += 1; t }

    def expectEnd(): Unit = {
      if (pos != tokens.length) throw new IllegalArgumentException(s"Unexpected token at end: ${tokens(pos).s}")
    }

    def parseExpr(): Column = parseOr()

    private def parseOr(): Column = {
      var left = parseAnd()
      while (peek.exists { case OP("OR") | OP("||") => true; case _ => false }) {
        next()
        left = left.or(parseAnd())
      }
      left
    }

    private def parseAnd(): Column = {
      var left = parsePred()
      while (peek.exists { case OP("AND") | OP("&&") => true; case _ => false }) {
        next()
        left = left.and(parsePred())
      }
      left
    }

    private def parsePred(): Column = peek match {
      case Some(LP(_)) =>
        next()
        val inner = parseExpr()
        expectRP()
        inner
      case _ =>
        parseComparison()
    }

    private def parseComparison(): Column = {
      val leftTerm = parseTerm()
      peek match {
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
          next()
          expectOp("IN")
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
      case Some(NUM(v))   =>
        next()
        Try(v.toLong).map(lit).getOrElse(Try(v.toDouble).map(lit).getOrElse(lit(v)))
      case other => throw new IllegalArgumentException(s"Expected term but found: ${other.map(_.s)}")
    }

    private def parseLiteralAny(): Any = peek match {
      case Some(STR(v)) => next(); v
      case Some(NUM(v)) =>
        next()
        Try(v.toLong).getOrElse(Try(v.toDouble).getOrElse(v))
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
// Dependency extraction (needed columns per table for DestTable)
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

      tableColRx.findAllMatchIn(cond).foreach { m =>
        add(m.group(1), m.group(2))
      }
    }
    acc.view.mapValues(_.toSet).toMap
  }
}

// ------------------------------
// Wide DF Builder: auto-select + auto-join with multi-hop + safe join keys
// ------------------------------
object WideBuilder {

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
      case None => keys.map(k => col(k))
      case Some(pfx) => keys.map(k => col(k).as(s"${pfx}${k}"))
    }

    val dataSelect: Seq[Column] =
      dataCols.map(c => col(c).as(s"$tableName.$c"))

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

          val joinCond = lk.zip(rk).map { case (lKey, rKey) =>
            wide.col(lKey) === rightDf.col(s"${pfx}${rKey}")
          }.reduce(_ && _)

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
// DestTableBuilder: apply mapping rules + create Text columns + output only PK + mapped cols
// ------------------------------
object DestTableBuilder {

  // treat SQL NULL (actual null) and the string "NULL" as missing
  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

  private def toInt(v: Any, default: Int): Int =
    Option(v).flatMap(x => Try(x.toString.trim.toInt).toOption).getOrElse(default)

  // token helpers
  private def isValueToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<VALUE>") || v.equalsIgnoreCase("&lt;VALUE&gt;"))

  // NEW: "<NULL>" token (text) means wildcard for SourceValue in ConversionType=2
  private def isNullTextToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<NULL>") || v.equalsIgnoreCase("&lt;NULL&gt;"))

  // Resolver:
  // - dot-safe (backticks)
  // - if unqualified, finds unique ".tail"
  // - if ambiguous, prefers SourceTableName if provided
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
          case 1 =>
            litCol(dottedMatches.head)
          case 0 =>
            throw new IllegalArgumentException(
              s"Cannot resolve column '$name0'. Try qualifying it (e.g. 'Unit.$tail'). " +
              s"Available examples: ${wideDf.columns.take(50).mkString(", ")}"
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
                  s"Ambiguous column '$tail' matches: ${dottedMatches.mkString(", ")}. " +
                  s"Qualify it in Condition (e.g. ${dottedMatches.head}) or set SourceTableName to drive resolution."
                )
            }
        }
      }
    }
  }

  // Build mapped DF for one destination table; returns ONLY PK + mapped columns (+Text)
  def buildForDestTable(destTableName: String, basePk: Seq[String], wideDf: DataFrame, mappingDf: DataFrame): DataFrame = {

    // Pull mapping rows for this destination table
    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTableName))
      .select(
        col("SourceTableName"),
        col("SourceColumnName"),
        col("SourceValue"),
        col("DestColumnName"),
        col("DestValue"),
        col("DestValueDescription"),  // NEW: text column source
        col("ConversionType"),
        col("Condition"),
        col("Priority")
      )
      .collect()
      .toSeq

    // Build rules grouped by DestColumnName
    val rulesByDestCol: Map[String, Seq[MapRule]] =
      rows
        .map { r =>
          MapRule(
            destColumn        = cleanOpt(r.getAs[Any]("DestColumnName")).getOrElse(""),
            sourceTable       = cleanOpt(r.getAs[Any]("SourceTableName")),
            sourceColumn      = cleanOpt(r.getAs[Any]("SourceColumnName")),
            sourceValueRaw    = cleanOpt(r.getAs[Any]("SourceValue")),
            destValueRaw      = cleanOpt(r.getAs[Any]("DestValue")),
            destValueDescRaw  = cleanOpt(r.getAs[Any]("DestValueDescription")),
            conversionType    = toInt(r.getAs[Any]("ConversionType"), 1),
            conditionRaw      = cleanOpt(r.getAs[Any]("Condition")),
            priority          = toInt(r.getAs[Any]("Priority"), 999999)
          )
        }
        .filter(r => r.destColumn.nonEmpty)
        .filter(r => r.conversionType == 1 || r.conversionType == 2)
        .groupBy(_.destColumn)
        .view.mapValues(_.sortBy(_.priority))
        .toMap

    var out = wideDf

    // Apply per destination column
    rulesByDestCol.toSeq.sortBy(_._1).foreach { case (destCol, rules) =>

      val destTextCol = s"${destCol}Text" // NEW: DestColumnName + "Text"

      // Build value expression (DestColumnName)
      val valueExpr: Column =
        rules.foldLeft(lit(null).cast("string")) { case (acc, r) =>
          val resRule: String => Column = resolver(out, r.sourceTable)

          // Source match logic:
          // - SourceValue = <VALUE> => wildcard (any)
          // - SourceValue = <NULL>  => NEW wildcard for ConversionType=2 (any)
          // - SourceValue SQL NULL  => match null
          // - else equality compare
          val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
            case (None, _) => lit(true)

            case (Some(_), sv) if isValueToken(sv) => lit(true)

            // NEW: for ConversionType=2 only, "<NULL>" means wildcard (any)
            case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true)

            case (Some(sc), None) => resRule(sc).isNull

            case (Some(sc), Some(v)) => resRule(sc) === lit(v)
          }

          // Condition parsing
          val condOk: Column = r.conditionRaw match {
            case None => lit(true)
            case Some(c) =>
              val cleaned = ConditionSanitizer.sanitize(c)
              try ConditionParser.parseToColumn(cleaned, resRule)
              catch {
                case e: Throwable =>
                  throw new IllegalArgumentException(
                    s"""Condition parse failed for Dest=${destTableName}.${destCol}, Priority=${r.priority}, SourceTable=${r.sourceTable.getOrElse("")}
                       |Original=<<<$c>>>
                       |Cleaned =<<<$cleaned>>>
                       |Error   =${e.getMessage}""".stripMargin, e
                  )
              }
          }

          val finalCond = srcOk && condOk

          // Destination value:
          // - DestValue <VALUE> => pass-through source column value
          // - else literal DestValue
          val destVal: Column =
            if (isValueToken(r.destValueRaw)) r.sourceColumn.map(resRule).getOrElse(lit(null))
            else r.destValueRaw.map(lit).getOrElse(lit(null))

          when(finalCond, destVal).otherwise(acc)
        }

      // Build text expression (DestColumnNameText)
      // If DestValueDescription is NULL for a rule, it simply won't set text for that rule.
      val textExpr: Column =
        rules.foldLeft(lit(null).cast("string")) { case (acc, r) =>
          val resRule: String => Column = resolver(out, r.sourceTable)

          val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
            case (None, _) => lit(true)
            case (Some(_), sv) if isValueToken(sv) => lit(true)
            case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true) // NEW
            case (Some(sc), None) => resRule(sc).isNull
            case (Some(sc), Some(v)) => resRule(sc) === lit(v)
          }

          val condOk: Column = r.conditionRaw match {
            case None => lit(true)
            case Some(c) =>
              val cleaned = ConditionSanitizer.sanitize(c)
              ConditionParser.parseToColumn(cleaned, resRule)
          }

          val finalCond = srcOk && condOk

          val destTextVal: Column = r.destValueDescRaw.map(lit).getOrElse(lit(null))
          when(finalCond, destTextVal).otherwise(acc)
        }

      // Add/merge columns (keep existing if present and mapping doesn't hit)
      out =
        if (out.columns.contains(destCol)) out.withColumn(destCol, coalesce(valueExpr, col(destCol)))
        else out.withColumn(destCol, valueExpr)

      out =
        if (out.columns.contains(destTextCol)) out.withColumn(destTextCol, coalesce(textExpr, col(destTextCol)))
        else out.withColumn(destTextCol, textExpr)
    }

    // FINAL OUTPUT: ONLY base PK + mapped columns (+Text), no extra wide columns
    val mappedCols: Seq[String] =
      rulesByDestCol.keys.toSeq.sorted.flatMap(dc => Seq(dc, s"${dc}Text")).distinct

    val pkCols: Seq[String] = basePk.distinct.filter(out.columns.contains)

    out.select((pkCols ++ mappedCols).map(c => col(s"`$c`")): _*)
  }
}

// =================================================================================================
// PUBLIC HELPER: build a destination output using graph + mapping table
// -------------------------------------------------------------------------------------------------
// Inputs:
//   destTableName: mapping DestTableName
//   baseTable:     the grain driver source table for this dest
//   graph:         TableGraph with DataFrames + PKs + join edges
//   mappingDf:     mapping table DF
//
// Output:
//   DataFrame containing ONLY base PK + mapped columns + mapped "Text" columns
// =================================================================================================
def buildDestTable(
  destTableName: String,
  baseTable: String,
  graph: TableGraph,
  mappingDf: DataFrame
): DataFrame = {
  val baseNode = graph.tables.getOrElse(baseTable, throw new IllegalArgumentException(s"Missing base table: $baseTable"))
  val wide = WideBuilder.buildWideDf(graph, mappingDf, destTableName, baseTable)
  DestTableBuilder.buildForDestTable(destTableName, baseNode.pk, wide, mappingDf)
}

// =================================================================================================
// USAGE EXAMPLE (adapt keys + edges to your real schema)
// -------------------------------------------------------------------------------------------------
// val dfUnit: DataFrame = ...
// val dfPersonInj: DataFrame = ...
// val dfCrash: DataFrame = ...
// val mappingDf: DataFrame = ...  // must include DestValueDescription column
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
