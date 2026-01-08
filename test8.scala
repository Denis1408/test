// =================================================================================================
// ONE-CELL FINAL SCRIPT (FIXED) — RETURNS Map[DestTableName -> DataFrame]
// -------------------------------------------------------------------------------------------------
// Fix included:
//   ✅ removed invalid ".curried" default arg
//   ✅ default tableLoader now correctly binds SparkSession: defaultTableLoader(spark): String => DataFrame
//
// Keeps:
//   ✅ auto load source tables inside loop
//   ✅ base table selection using sourceTablePriority
//   ✅ case-insensitive table/column handling (UNIT vs Unit, etc)
//   ✅ conditions with "=NULL" / "!=NULL" + IS NULL / IS NOT NULL
//   ✅ <DEFAULT> fallback, <NULL> wildcard for type2, <VALUE> pass-through
//   ✅ add Id when destination uses >1 source table (Id built from PKs across involved tables)
//   ✅ final DF per destination contains only base PK + mapped columns (+Text) + optional Id
// =================================================================================================

import org.apache.spark.sql.{DataFrame, Column, SparkSession}
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
// Dependency extraction (tables/cols in Conditions can be CAPS)
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
// Wide DF Builder (canonical table names; keepAllPk option for Id hashing)
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
// DestTableBuilder (case-insensitive resolver; tokens; default fallback; Text columns)
// ------------------------------
object DestTableBuilder {

  private def cleanOpt(v: Any): Option[String] =
    Option(v).map(_.toString).map(_.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))

  private def toInt(v: Any, default: Int): Int =
    Option(v).flatMap(x => Try(x.toString.trim.toInt).toOption).getOrElse(default)

  private def isValueToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<VALUE>") || v.equalsIgnoreCase("&lt;VALUE&gt;"))

  private def isNullTextToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<NULL>") || v.equalsIgnoreCase("&lt;NULL&gt;"))

  private def isDefaultToken(s: Option[String]): Boolean =
    s.exists(v => v.equalsIgnoreCase("<DEFAULT>") || v.equalsIgnoreCase("&lt;DEFAULT&gt;"))

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

      // Type 2 extra: SourceValue == "<NULL>" => wildcard, accept any source value (incl null)
      val srcOk: Column = (r.sourceColumn, r.sourceValueRaw) match {
        case (None, _) => lit(true)
        case (Some(_), sv) if isValueToken(sv) => lit(true)
        case (Some(_), sv) if r.conversionType == 2 && isNullTextToken(sv) => lit(true)
        case (Some(sc), None) => resRule(sc).isNull
        case (Some(sc), Some(v)) => resRule(sc) === lit(v)
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

      val raw = valueSelector(r)
      val destVal: Column =
        if (passThroughOnValueToken && isValueToken(raw)) r.sourceColumn.map(resRule).getOrElse(lit(null))
        else raw.map(lit).getOrElse(lit(null))

      val effectiveCond = (srcOk && condOk) && (if (whenOnlyIfAccNull) acc.isNull else lit(true))
      when(effectiveCond, destVal).otherwise(acc)
    }
  }

  // returns: (mappedDf, mappedCols)
  def buildForDestTable(destTableName: String, basePk: Seq[String], wideDf: DataFrame, mappingDf: DataFrame): (DataFrame, Seq[String]) = {

    val rows = mappingDf
      .filter(col("DestTableName") === lit(destTableName))
      .select("SourceTableName","SourceColumnName","SourceValue","DestColumnName","DestValue","DestValueDescription","ConversionType","Condition","Priority")
      .collect()
      .toSeq

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

      // DEFAULT rules: SourceColumnName is NULL and SourceValue == "<DEFAULT>"
      val (defaultRules, normalRules) =
        allRulesSorted.partition(r => r.sourceColumn.isEmpty && isDefaultToken(r.sourceValueRaw))

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
// AUTOMATION: returns Map[DestTableName -> DataFrame]
// =================================================================================================
object MappingAutomation {

  // ✅ FIXED: proper SparkSession-bound default loader (NO currying on DataFrame)
  def defaultTableLoader(spark: SparkSession): String => DataFrame =
    (tableName: String) => spark.table(tableName)

  private def canon(name: String, universe: Set[String], label: String): String =
    universe.find(_.equalsIgnoreCase(name))
      .getOrElse(throw new IllegalArgumentException(s"Unknown $label '$name'. Known: ${universe.toSeq.sorted.mkString(", ")}"))

  private def chooseBaseTable(sourceTables: Set[String], sourceTablePriority: Map[String, Int]): String = {
    val prioDefault = 999999
    sourceTables.toSeq.sortBy { t =>
      sourceTablePriority.find { case (k, _) => k.equalsIgnoreCase(t) }.map(_._2).getOrElse(prioDefault) -> t.toLowerCase
    }.head
  }

  private def buildGraphForTables(
    spark: SparkSession,
    tableLoader: String => DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdges: Seq[JoinEdge],
    tablesNeededRaw: Set[String]
  ): TableGraph = {

    val pkUniverse = pkByTable.keySet
    val tablesNeeded = tablesNeededRaw.map(t => canon(t, pkUniverse, "table"))

    val nodes: Map[String, TableNode] = tablesNeeded.map { t =>
      val df = tableLoader(t)
      val pk = pkByTable(t)
      val missing = pk.filterNot(df.columns.contains)
      if (missing.nonEmpty) throw new IllegalArgumentException(s"PK columns missing in table $t: ${missing.mkString(", ")}")
      t -> TableNode(df, pk)
    }.toMap

    val canonEdges = joinEdges.map { e =>
      val l = canon(e.leftTable, pkUniverse, "table")
      val r = canon(e.rightTable, pkUniverse, "table")
      e.copy(leftTable = l, rightTable = r)
    }.filter(e => nodes.contains(e.leftTable) && nodes.contains(e.rightTable))

    TableGraph(nodes, canonEdges)
  }

  private def addIdIfNeeded(destName: String, baseTable: String, involvedTables: Set[String], graph: TableGraph, wideDf: DataFrame, mappedDf: DataFrame): DataFrame = {
    if (involvedTables.size <= 1) return mappedDf

    val cols = wideDf.columns.toSet

    def pkColsForTable(t: String): Seq[String] = {
      val node = graph.tables(t)
      if (t.equalsIgnoreCase(baseTable)) node.pk.filter(cols.contains)
      else {
        val pfx = s"__k__${t}__"
        node.pk.map(k => s"${pfx}${k}").filter(cols.contains)
      }
    }

    val pkAll = involvedTables.toSeq.sortBy(_.toLowerCase).flatMap(pkColsForTable).distinct
    if (pkAll.isEmpty) return mappedDf

    val idExpr = sha2(concat_ws("||", pkAll.map(c => coalesce(col(s"`$c`").cast("string"), lit(""))): _*), 256)

    val basePk = graph.tables(baseTable).pk.filter(mappedDf.columns.contains)
    if (basePk.isEmpty) mappedDf.withColumn("Id", idExpr)
    else {
      val idDf = wideDf.select(basePk.map(c => col(s"`$c`")) :+ idExpr.as("Id"): _*).dropDuplicates(basePk)
      mappedDf.join(idDf, basePk, "left")
    }
  }

  // MAIN: returns Map[DestTableName -> DataFrame]
  def buildAllDestTablesAsMap(
    spark: SparkSession,
    mappingDf: DataFrame,
    pkByTable: Map[String, Seq[String]],
    joinEdges: Seq[JoinEdge],
    sourceTablePriority: Map[String, Int] = Map.empty,
    destTablePriority: Map[String, Int] = Map.empty,
    tableLoader: String => DataFrame = defaultTableLoader(spark)   // ✅ FIXED DEFAULT ARG
  ): Map[String, DataFrame] = {

    val destTables = mappingDf.select(col("DestTableName")).distinct().collect().map(_.getString(0)).toSeq
    val destSorted = destTables.sortBy { d =>
      destTablePriority.find { case (k, _) => k.equalsIgnoreCase(d) }.map(_._2).getOrElse(999999) -> d.toLowerCase
    }

    val pkUniverse = pkByTable.keySet

    val outPairs = destSorted.map { dest =>
      val neededTablesRaw = RuleDependency.requiredTableColsForDest(mappingDf, dest).keySet

      val srcFromMappingRaw = mappingDf
        .filter(col("DestTableName") === lit(dest))
        .select(col("SourceTableName"))
        .distinct()
        .collect()
        .map(_.getString(0))
        .filter(_ != null)
        .toSet

      val involvedTables = (neededTablesRaw ++ srcFromMappingRaw).filter(_ != null).map(t => canon(t, pkUniverse, "table"))

      val graph = buildGraphForTables(spark, tableLoader, pkByTable, joinEdges, involvedTables)

      val baseCandidates =
        if (srcFromMappingRaw.nonEmpty) srcFromMappingRaw.map(t => canon(t, pkUniverse, "table"))
        else involvedTables

      val baseTable = chooseBaseTable(baseCandidates, sourceTablePriority)

      val (wide, baseCanon, _) = WideBuilder.buildWideDf(graph, mappingDf, dest, baseTable, keepAllPk = true)

      val (mappedOnly, _) = DestTableBuilder.buildForDestTable(dest, graph.tables(baseCanon).pk, wide, mappingDf)

      val finalDf = addIdIfNeeded(dest, baseCanon, involvedTables, graph, wide, mappedOnly)

      dest -> finalDf
    }

    outPairs.toMap
  }
}

// =================================================================================================
// USAGE EXAMPLE
// -------------------------------------------------------------------------------------------------
// val outMap: Map[String, DataFrame] = MappingAutomation.buildAllDestTablesAsMap(
//   spark = spark,
//   mappingDf = mappingDf,
//   pkByTable = pkByTable,
//   joinEdges = joinEdges,
//   sourceTablePriority = sourceTablePriority
// )
//
// outMap("Vehicle").show(false)
// outMap("Person").show(false)
// =================================================================================================
