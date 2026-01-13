// =================================================================================================
// FINAL ONE-CELL SCRIPT (WITH LAST-STEP COLUMN RENAME SUPPORT + DETAILED COMMENTS)
// -------------------------------------------------------------------------------------------------
// NEW FEATURE (LAST STEP):
// ✅ Ability to rename columns in EACH result destination DataFrame.
// ✅ User provides rename map like: Map("Unit_id" -> "Vehicle_id").
// ✅ Rename happens ONLY IF:
//    1) old column exists in the result DF (case-insensitive)
//    2) new column does NOT already exist in the result DF (case-insensitive)
// ✅ Rename is applied as the VERY LAST STEP (after mapping, multi-source union, Id, DestNameId, etc)
//
// HOW TO USE:
//   val renameRulesByDest: Map[String, Map[String,String]] = Map(
//     "Person" -> Map("Unit_id" -> "Vehicle_id")
//   )
//
//   val outMap = MappingAutomation.buildAllDestTablesAsMap(
//     spark, mappingDf, pkByTable, joinEdges,
//     sourceTablePriority = sourceTablePriority,
//     excludeDestIdFor = Set("Address"),
//     renameRulesByDest = renameRulesByDest
//   )
//
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
  leftTable: String,                 // source table name (as in pkByTable)
  rightTable: String,                // source table name (as in pkByTable)
  leftKeys: Seq[String],             // join keys on left table
  rightKeys: Seq[String],            // join keys on right table
  joinType: String = "left"          // join type (normally "left")
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
  destColumn: String,                // DestColumnName
  sourceTable: Option[String],        // SourceTableName
  sourceColumn: Option[String],       // SourceColumnName
  sourceValueRaw: Option[String],     // SourceValue (supports NULL / <NULL> / <DEFAULT> / <VALUE>)
  destValueRaw: Option[String],       // DestValue (supports <VALUE> / <FORMAT>)
  destValueDescRaw: Option[String],   // DestValueDescription -> will be written into DestColumnText
  conversionType: Int,               // ConversionType (supported 1 and 2)
  conditionRaw: Option[String],       // Condition (boolean expression OR format string for <FORMAT>)
  priority: Int                       // Priority (lower number = higher priority)
)

// =================================================================================================
// 1) CONDITION SANITIZER
//    Cleans common issues from mapping condition strings.
// =================================================================================================
object ConditionSanitizer {
  def sanitize(raw: String): String = {
    if (raw == null) return null
    var s = raw.trim

    // Normalize “smart quotes” to normal quotes.
    s = s.replace('\u201C','"').replace('\u201D','"').replace('\u2018','\'').replace('\u2019','\'')

    // Strip trailing semicolons.
    while (s.endsWith(";")) s = s.dropRight(1).trim

    // Remove dangling boolean operators that would break parsing.
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

    // Balance parentheses if condition ended with missing closing parens.
    val open = s.count(_ == '(')
    val close = s.count(_ == ')')
    if (open > close) s = s + (")" * (open - close))

    s
  }
}

// =================================================================================================
// 2) CONDITION PARSER
//    Converts SQL-like expressions into Spark Column filter expressions.
//    Supports:
//      - AND/OR/&&/||
//      - IN / NOT IN
//      - IS NULL / IS NOT NULL
//      - =NULL / !=NULL  (treated as IS NULL / IS NOT NULL)
//      - parentheses
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

  // --- Simple tokenizer for condition expressions
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

  // --- Recursive descent parser: OR -> AND -> predicate -> comparison/term
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
        // Treat "= NULL" and "== NULL" as IS NULL
        case Some(OP("=" | "==")) if peek2.contains(OP("NULL")) => next(); next(); leftTerm.isNull
        // Treat "!= NULL" and "<> NULL" as IS NOT NULL
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
// 3) RULE DEPENDENCY EXTRACTION
//    Finds which source tables/columns are needed for a given destination table.
//    It reads:
//      - SourceTableName/SourceColumnName
//      - also parses Condition for tokens "Table.Column"
// =================================================================================================
object RuleDependency {
  private val tableColRx: Regex = """([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)""".r

  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

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

      // Parse Condition for Table.Column references
      tableColRx.findAllMatchIn(cond).foreach(m => add(m.group(1), m.group(2)))
    }

    acc.view.mapValues(_.toSet).toMap
  }
}

// =================================================================================================
// 4) WIDE BUILDER
//    Builds a single wide DF that includes columns from all required source tables.
//    - Base table columns kept normally
//    - For joined tables, PK columns are kept with prefix __k__<table>__
//    - Data columns are kept as "<table>.<column>" to avoid collisions
// =================================================================================================
object WideBuilder {

  // Canonicalize table name by case-insensitive match against graph tables.
  private def canonicalTableName(graph: TableGraph, t: String): String =
    graph.tables.keys.find(_.equalsIgnoreCase(t))
      .getOrElse(throw new IllegalArgumentException(s"Unknown table '$t'. Known: ${graph.tables.keys.toSeq.sorted.mkString(", ")}"))

  // Select only required cols + PKs; apply aliases to avoid collisions.
  private def selectWithAliases(
    tableName: String,
    df: DataFrame,
    cols: Set[String],
    keepKeys: Seq[String],
    keyPrefix: Option[String]
  ): DataFrame = {

    val keys = keepKeys.distinct.filter(df.columns.contains)
    val dataCols = cols.filter(df.columns.contains).toSeq.distinct.sorted

    // PK selection (maybe prefixed)
    val keySelect: Seq[Column] = keyPrefix match {
      case None       => keys.map(col)
      case Some(pfx)  => keys.map(k => col(k).as(s"${pfx}${k}"))
    }

    // Data selection always as tableName.colName
    val dataSelect: Seq[Column] = dataCols.map(c => col(c).as(s"$tableName.$c"))
    val sel = keySelect ++ dataSelect

    if (sel.isEmpty) df else df.select(sel: _*)
  }

  // Build adjacency list for BFS path finding
  private def buildAdj(edges: Seq[JoinEdge]): Map[String, Seq[JoinEdge]] =
    edges.flatMap(e => Seq(e.leftTable -> e, e.rightTable -> e))
      .groupBy(_._1).view.mapValues(_.map(_._2)).toMap

  // Find join path between tables using BFS
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

  // Determine which side keys to use for a given direction along JoinEdge
  private def edgeKeys(from: String, to: String, e: JoinEdge): (Seq[String], Seq[String]) = {
    if (e.leftTable == from && e.rightTable == to) (e.leftKeys, e.rightKeys)
    else if (e.rightTable == from && e.leftTable == to) (e.rightKeys, e.leftKeys)
    else throw new IllegalArgumentException(s"Edge does not connect $from -> $to : $e")
  }

  // Build wide DF using base table and joining required tables along graph paths
  def buildWideDf(
    graph: TableGraph,
    mappingDf: DataFrame,
    destTable: String,
    baseTable: String,
    keepAllPk: Boolean
  ): (DataFrame, String, Set[String]) = {

    // Required table->cols from mapping+conditions
    val reqRaw = RuleDependency.requiredTableColsForDest(mappingDf, destTable)
    val base = canonicalTableName(graph, baseTable)

    // Canonicalize req table names
    val req: Map[String, Set[String]] =
      reqRaw.toSeq
        .groupBy { case (t, _) => canonicalTableName(graph, t) }
        .map { case (canonT, entries) => canonT -> entries.flatMap(_._2).toSet }

    val neededTables: Seq[String] = (req.keySet + base).toSeq.distinct
    val baseNode = graph.tables(base)

    // Start wide DF from base table (PKs + required cols)
    var wide = selectWithAliases(base, baseNode.df, req.getOrElse(base, Set.empty), baseNode.pk, None)
    val joined = scala.collection.mutable.Set[String](base)

    // Helper: select right table DF with prefixed PK columns and dotted data columns
    def getTableDf(t: String): (DataFrame, String, Seq[String]) = {
      val node = graph.tables(t)
      val pfx  = s"__k__${t}__"
      val dfSel = selectWithAliases(t, node.df, req.getOrElse(t, Set.empty), node.pk, Some(pfx))
      (dfSel, pfx, node.pk)
    }

    // Join every needed table into wide DF
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

          // wide.col(lk) == rightDf.col(prefixed rk)
          val joinCond = lk.zip(rk).map { case (lKey, rKey) =>
            wide.col(lKey) === rightDf.col(s"${pfx}${rKey}")
          }.reduce(_ && _)

          val joinedDf = wide.join(rightDf, joinCond, e.joinType)

          // If keepAllPk==false we can drop right PK cols, but for our pipeline we keepAllPk==true.
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

// =================================================================================================
// 5) DEST TABLE BUILDER
//    Applies mapping rules to a wide DF to create destination columns.
//    - Supports ConversionType 1 & 2
//    - Supports <VALUE> pass-through
//    - Supports <DEFAULT> fallback (applied only if result still null)
//    - Type2 special wildcard behavior:
//        SourceValue NULL OR <NULL> => treat as ANY value (condition decides)
//    - Type1 <FORMAT> behavior:
//        SourceValue <VALUE> and DestValue <FORMAT>
//        Condition column contains a date_format pattern used after parsing string safely
//    - Adds DestValueDescription into new column: <DestColumnName>Text
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

  // Safely parse string date/time into timestamp by trying multiple common patterns.
  private def safeToTimestamp(c: Column): Column = {
    val s = c.cast("string")
    coalesce(
      // Date
      try_to_timestamp(s, lit("MM/dd/yyyy")),
      try_to_timestamp(s, lit("M/d/yyyy")),
      try_to_timestamp(s, lit("MM-dd-yyyy")),
      try_to_timestamp(s, lit("M-d-yyyy")),
      try_to_timestamp(s, lit("yyyy-MM-dd")),
      try_to_timestamp(s, lit("yyyy/MM/dd")),
      try_to_timestamp(s, lit("yyyyMMdd")),
      // DateTime
      try_to_timestamp(s, lit("MM/dd/yyyy HH:mm:ss")),
      try_to_timestamp(s, lit("MM/dd/yyyy H:mm:ss")),
      try_to_timestamp(s, lit("MM/dd/yyyy HH:mm")),
      try_to_timestamp(s, lit("MM/dd/yyyy H:mm")),
      // Time-only -> attach dummy date
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd HH:mm:ss")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd H:mm:ss")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd HH:mm")),
      try_to_timestamp(concat(lit("1970-01-01 "), s), lit("yyyy-MM-dd H:mm")),
      // Last resort (Spark's parser)
      try_to_timestamp(s)
    )
  }

  // Case-insensitive resolver for column names against wideDf schema.
  private def resolver(wideDf: DataFrame, defaultTable: Option[String]): String => Column = {
    val cols = wideDf.columns
    val lowerToActual: Map[String, String] = cols.groupBy(_.toLowerCase).map { case (k, vs) => k -> vs.head }

    (name0: String) => {
      val name = name0.trim
      val nameL = name.toLowerCase
      val tail = name.split('.').last
      val tailL = tail.toLowerCase
      def litCol(actual: String): Column = col(s"`$actual`")

      // Direct match by full name, or tail-only match, or unique dotted match.
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

  // Build expression for a single destination column from ordered rules.
  private def buildExpr(
    destTableName: String,
    destCol: String,
    rules: Seq[MapRule],
    outDf: DataFrame,
    whenOnlyIfAccNull: Boolean,             // true only for <DEFAULT> fallback
    valueSelector: MapRule => Option[String],
    passThroughOnValueToken: Boolean
  ): Column = {

    rules.foldLeft(lit(null).cast("string")) { case (acc, r) =>
      val resRule: String => Column = resolver(outDf, r.sourceTable)

      // Type1 special: SourceValue=<VALUE> AND DestValue=<FORMAT>
      // In this case, "Condition" contains format pattern for date_format().
      val fmtRule: Boolean =
        r.conversionType == 1 && isValueToken(r.sourceValueRaw) && isFormatToken(r.destValueRaw)

      // Decide if the source matches the rule.
      // Key fixes:
      // - Type2: SourceValue NULL or <NULL> => wildcard (any source value) - only condition matters
      val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
        case (None, _) => lit(true)

        case (Some(_), sv) if isValueToken(sv) => lit(true)

        case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true)

        case (Some(_), None) if r.conversionType == 2 => lit(true) // ✅ Type2 SourceValue NULL => wildcard

        case (Some(sc), None) => resRule(sc).isNull                 // Type1 SourceValue NULL => isNull

        case (Some(sc), Some(v)) => resRule(sc) === lit(v)
      }

      // Condition:
      // - for fmtRule: condition is format string (not boolean)
      // - otherwise: parse boolean condition (supports NULL variants)
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

      // Compute destination value for this rule.
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

      // Apply rule only if:
      // - srcOk AND condOk
      // - AND (if default step) accumulator is still null
      val effectiveCond = (srcOk && condOk) && (if (whenOnlyIfAccNull) acc.isNull else lit(true))
      when(effectiveCond, destVal).otherwise(acc)
    }
  }

  // Build final mapped DF for one base table pass (only base PKs + mapped columns).
  def buildForDestTable(
    destTableName: String,
    baseTable: String,
    basePk: Seq[String],
    wideDf: DataFrame,
    mappingDf: DataFrame
  ): (DataFrame, Seq[String]) = {

    val baseLower = baseTable.toLowerCase

    // Filter rules to only those that belong to this base source table (plus <DEFAULT>)
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

    // Group rules by destination column, order by priority
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

    // Apply column mapping
    var out = wideDf
    rulesByDestCol.toSeq.sortBy(_._1).foreach { case (destCol, allRulesSorted) =>
      val destTextCol = s"${destCol}Text"

      // default rules are those with SourceColumnName null and SourceValue <DEFAULT>
      val (defaultRules, normalRules) = allRulesSorted.partition(r => r.sourceColumn.isEmpty && isDefaultToken(r.sourceValueRaw))

      // Build value expression (normal first, then default if still null)
      val normalVal = buildExpr(destTableName, destCol, normalRules, out, whenOnlyIfAccNull = false, _.destValueRaw, passThroughOnValueToken = true)
      val defVal    = buildExpr(destTableName, destCol, defaultRules, out, whenOnlyIfAccNull = true,  _.destValueRaw, passThroughOnValueToken = true)
      val finalVal  = coalesce(normalVal, defVal)

      // Build text expression (DestValueDescription)
      val normalTxt = buildExpr(destTableName, destCol, normalRules, out, whenOnlyIfAccNull = false, _.destValueDescRaw, passThroughOnValueToken = false)
      val defTxt    = buildExpr(destTableName, destCol, defaultRules, out, whenOnlyIfAccNull = true,  _.destValueDescRaw, passThroughOnValueToken = false)
      val finalTxt  = coalesce(normalTxt, defTxt)

      out = out.withColumn(destCol, finalVal).withColumn(destTextCol, finalTxt)
    }

    // Only base PK columns + mapped columns (and their Text columns)
    val mappedCols: Seq[String] = rulesByDestCol.keys.toSeq.sorted.flatMap(dc => Seq(dc, s"${dc}Text")).distinct
    val pkCols: Seq[String] = basePk.distinct.filter(out.columns.contains)

    val projected = out.select((pkCols ++ mappedCols).map(c => col(s"`$c`")): _*)
    (projected, mappedCols)
  }
}

// =================================================================================================
// 6) MAPPING AUTOMATION
//    - Reads distinct destination tables from mappingDf
//    - For each destination:
//        * find involved source tables (mapping + condition references)
//        * for EACH involved source table (ordered by priority):
//             - build wide DF starting from that base
//             - build mapped DF for that base
//             - add stable Id + _BaseSourceTable (no dropping duplicates)
//        * unionByName all bases => destination output (keeps all rows)
//        * add <DestName>Id (optional, can exclude)
//        * apply FINAL renames (NEW FEATURE)  <<< THIS IS YOUR REQUEST
// =================================================================================================
object MappingAutomation {

  // Default way to load source table by name (override if you want schema/catalog prefix)
  def defaultTableLoader(spark: SparkSession): String => DataFrame =
    (tableName: String) => spark.table(tableName)

  // Some users pass joinEdges as tuple-like objects; normalize them into JoinEdge
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

  // Case-insensitive membership test
  private def inUniverseCI(name: String, universe: Set[String]): Boolean =
    universe.exists(_.equalsIgnoreCase(name))

  // Canonicalize a source table name (case-insensitive) against pkByTable keys
  private def canonSource(name: String, universe: Set[String], label: String): String =
    universe.find(_.equalsIgnoreCase(name))
      .getOrElse(throw new IllegalArgumentException(
        s"Unknown $label '$name'. Known SOURCE tables: ${universe.toSeq.sorted.mkString(", ")}"
      ))

  // Build graph with only tables we actually need for this destination
  private def buildGraphForTables(
    tableLoader: String => DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdges: Seq[JoinEdge],
    sourceTablesNeeded: Set[String]
  ): TableGraph = {

    // Load each needed table as DF and validate PK existence
    val nodes: Map[String, TableNode] = sourceTablesNeeded.map { t =>
      val df = tableLoader(t)
      val pk = pkByTable(t)
      val missing = pk.filterNot(df.columns.contains)
      if (missing.nonEmpty) throw new IllegalArgumentException(s"PK columns missing in SOURCE table $t: ${missing.mkString(", ")}")
      t -> TableNode(df, pk)
    }.toMap

    // Keep only edges fully within this destination's required table set
    val edgesFiltered = joinEdges.filter(e => nodes.contains(e.leftTable) && nodes.contains(e.rightTable))
    TableGraph(nodes, edgesFiltered)
  }

  // Add stable Id for each row of a base pass WITHOUT dropping duplicates.
  // Also add marker column `_BaseSourceTable` so later ordering is deterministic.
  private def addIdWithoutDroppingDuplicates(baseTable: String, graph: TableGraph, mappedDf: DataFrame): DataFrame = {
    val basePk = graph.tables(baseTable).pk.filter(mappedDf.columns.contains)
    val rnColName = "__rn__"

    // Deterministic ordering inside each PK partition (based on row hash)
    val orderExpr = xxhash64(mappedDf.columns.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))): _*)

    // Row_number inside PK partition (or global if no PK)
    val withRn =
      if (basePk.nonEmpty) {
        val w = Window.partitionBy(basePk.map(c => col(s"`$c`")): _*).orderBy(orderExpr)
        mappedDf.withColumn(rnColName, row_number().over(w))
      } else {
        val w = Window.orderBy(orderExpr)
        mappedDf.withColumn(rnColName, row_number().over(w))
      }

    // Id = sha2( PK values + baseTable + row_number )
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

  // Add <DestTableName>Id at the very end, partitioned by Crash_Id.
  // This creates per-crash sequential ids 1..N across ALL unioned source rows.
  // If dest table is excluded, return df unchanged.
  private def addDestTableIdAtEnd(
    destName: String,
    df: DataFrame,
    involvedSourceTables: Seq[String],
    pkByTable: Map[String, Seq[String]],
    excludeDestIdFor: Set[String],
    crashIdLogical: String = "Crash_Id"
  ): DataFrame = {

    // Exclusion (case-insensitive)
    if (excludeDestIdFor.exists(_.equalsIgnoreCase(destName))) return df

    // Case-insensitive column resolution
    val lowerToActual = df.columns.groupBy(_.toLowerCase).map { case (k, vs) => k -> vs.head }

    // Crash_Id must exist
    val crashCol = lowerToActual.get(crashIdLogical.toLowerCase).getOrElse(
      throw new IllegalArgumentException(
        s"Destination '$destName' DF missing Crash_Id (case-insensitive). Existing: ${df.columns.sorted.mkString(", ")}"
      )
    )

    // Use all PK columns from involved source tables if present in df (case-insensitive)
    val allPkLogical = involvedSourceTables.flatMap(t => pkByTable.getOrElse(t, Nil)).distinct
    val allPkActual  = allPkLogical.flatMap(pk => lowerToActual.get(pk.toLowerCase)).distinct

    // Include base source marker in ordering if exists
    val baseSourceActualOpt = lowerToActual.get("_basesourcetable")

    val orderCols: Seq[Column] =
      (baseSourceActualOpt.toSeq.map(c => col(s"`$c`")) ++
        allPkActual.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))))

    val w =
      if (orderCols.nonEmpty) Window.partitionBy(col(s"`$crashCol`")).orderBy(orderCols: _*)
      else Window.partitionBy(col(s"`$crashCol`")).orderBy(lit(1))

    // ✅ Column name without underscore
    val destIdCol = s"${destName}Id"
    val df2 = df.withColumn(destIdCol, row_number().over(w))

    // Ensure the new Id column is appended at the end
    val colsNoNew = df2.columns.filterNot(_.equalsIgnoreCase(destIdCol))
    df2.select((colsNoNew.map(c => col(s"`$c`")) :+ col(s"`$destIdCol`")): _*)
  }

  // ----------------------------------------------------------------------------------------------
  // NEW FEATURE: LAST-STEP COLUMN RENAME
  // ----------------------------------------------------------------------------------------------
  // Rules:
  // - Apply rename only if old column exists (case-insensitive)
  // - Do NOT rename if new name already exists (case-insensitive)
  // - Can rename multiple columns sequentially
  // - Runs after ALL other transformations are complete
  // ----------------------------------------------------------------------------------------------
  private def applySafeRenamesLast(
    df: DataFrame,
    renameMap: Map[String, String]
  ): DataFrame = {

    if (renameMap == null || renameMap.isEmpty) return df

    // For case-insensitive checks
    def lowerSet(cols: Array[String]) = cols.map(_.toLowerCase).toSet

    renameMap.foldLeft(df) { case (curDf, (oldNameRaw, newNameRaw)) =>
      val oldName = Option(oldNameRaw).map(_.trim).getOrElse("")
      val newName = Option(newNameRaw).map(_.trim).getOrElse("")
      if (oldName.isEmpty || newName.isEmpty) curDf
      else {
        val cols = curDf.columns
        val colsLower = lowerSet(cols)

        // Find actual existing old column name by case-insensitive match
        val oldActualOpt = cols.find(_.equalsIgnoreCase(oldName))

        // Only rename if old exists AND new does not exist
        if (oldActualOpt.isDefined && !colsLower.contains(newName.toLowerCase)) {
          curDf.withColumnRenamed(oldActualOpt.get, newName)
        } else {
          curDf
        }
      }
    }
  }

  // =================================================================================================
  // MAIN ENTRYPOINT
  // Returns: Map[DestTableName -> DataFrame]
  // =================================================================================================
  def buildAllDestTablesAsMap(
    spark: SparkSession,
    mappingDf: DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdgesRaw: Seq[_],
    sourceTablePriority: Map[String, Int] = Map.empty,           // base table ordering within a destination
    destTablePriority: Map[String, Int] = Map.empty,             // destination processing ordering
    excludeDestIdFor: Set[String] = Set.empty,                   // exclude <DestName>Id
    renameRulesByDest: Map[String, Map[String,String]] = Map.empty, // ✅ NEW: per-destination rename rules
    tableLoader: String => DataFrame = defaultTableLoader(_:SparkSession).curried(spark)
  ): Map[String, DataFrame] = {

    // Universe of valid source tables = keys of pkByTable
    val sourceUniverse = pkByTable.keySet

    // Normalize join edges and canonicalize table names to match pkByTable keys
    val joinEdgesNorm0 = normalizeJoinEdges(joinEdgesRaw)
    val joinEdges: Seq[JoinEdge] =
      joinEdgesNorm0
        .filter(e => inUniverseCI(e.leftTable, sourceUniverse) && inUniverseCI(e.rightTable, sourceUniverse))
        .map { e =>
          val l = canonSource(e.leftTable, sourceUniverse, "SOURCE table")
          val r = canonSource(e.rightTable, sourceUniverse, "SOURCE table")
          e.copy(leftTable = l, rightTable = r)
        }

    // Find all destination tables to build
    val destTables = mappingDf.select(col("DestTableName")).distinct().collect().map(_.getString(0)).toSeq

    // Order destination tables by optional destTablePriority
    val destSorted = destTables.sortBy { d =>
      destTablePriority.find { case (k, _) => k.equalsIgnoreCase(d) }.map(_._2).getOrElse(999999) -> d.toLowerCase
    }

    // Build each destination DataFrame
    val outPairs: Seq[(String, DataFrame)] = destSorted.map { dest =>

      // Tables referenced by conditions + columns
      val neededTablesRaw = RuleDependency.requiredTableColsForDest(mappingDf, dest).keySet

      // Tables referenced directly in mapping SourceTableName
      val srcFromMappingRaw = mappingDf
        .filter(col("DestTableName") === lit(dest))
        .select(col("SourceTableName"))
        .distinct()
        .collect()
        .map(_.getString(0))
        .filter(_ != null)
        .toSet

      // Combine both and keep only those that exist in pkByTable (source universe)
      val involvedSourceTables: Seq[String] =
        (neededTablesRaw ++ srcFromMappingRaw)
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

      // Build graph for just the needed source tables
      val graph = buildGraphForTables(tableLoader, pkByTable, joinEdges, involvedSourceTables.toSet)

      // Order base sources by sourceTablePriority
      val baseTablesOrdered: Seq[String] =
        involvedSourceTables.sortBy { t =>
          sourceTablePriority.find { case (k, _) => k.equalsIgnoreCase(t) }.map(_._2).getOrElse(999999) -> t.toLowerCase
        }

      // For each base, build a mapped DF (only base PKs + mapped columns), then add stable Id
      val perBase: Seq[DataFrame] = baseTablesOrdered.map { baseTable =>
        val (wide, baseCanon, _) = WideBuilder.buildWideDf(graph, mappingDf, dest, baseTable, keepAllPk = true)
        val (mappedOnly, _) = DestTableBuilder.buildForDestTable(dest, baseCanon, graph.tables(baseCanon).pk, wide, mappingDf)
        addIdWithoutDroppingDuplicates(baseCanon, graph, mappedOnly)
      }

      // Union all base results to keep ALL records (never overwrite by crash_id)
      val unioned =
        if (perBase.size == 1) perBase.head
        else perBase.reduce(_.unionByName(_, allowMissingColumns = true))

      // Add <DestName>Id (unless excluded)
      val withDestId = addDestTableIdAtEnd(
        destName = dest,
        df = unioned,
        involvedSourceTables = involvedSourceTables,
        pkByTable = pkByTable,
        excludeDestIdFor = excludeDestIdFor,
        crashIdLogical = "Crash_Id"
      )

      // ✅ NEW: apply column renames as the very LAST step
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
//   "Person"  -> Map("Unit_id" -> "Vehicle_id"),         // rename only if Unit_id exists and Vehicle_id doesn't
//   "Vehicle" -> Map("SomeOld" -> "SomeNew")
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
// display(outMap("Person"))
// =================================================================================================
