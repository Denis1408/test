import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.Try
import scala.util.matching.Regex

// ============================================================================
// MODELS
// ============================================================================
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

// ============================================================================
// CONDITION SANITIZER
// ============================================================================
object ConditionSanitizer {
  def sanitize(raw: String): String = {
    if (raw == null) return null
    var s = raw.trim
      .replace('\u201C','"').replace('\u201D','"')
      .replace('\u2018','\'').replace('\u2019','\'')
    while (s.endsWith(";")) s = s.dropRight(1).trim
    val bad = Seq("AND","OR","&&","||","(")
    var changed = true
    while (changed) {
      changed = false
      bad.find(b => s.toUpperCase.endsWith(b)).foreach { b =>
        s = s.dropRight(b.length).trim
        changed = true
      }
    }
    val diff = s.count(_=='(') - s.count(_==')')
    if (diff > 0) s = s + (")"*diff)
    s
  }
}

// ============================================================================
// CONDITION PARSER (NULL SAFE, CAPS SAFE)
// ============================================================================
object ConditionParser {
  sealed trait Tok { def s: String }
  case class ID(s:String) extends Tok
  case class STR(s:String) extends Tok
  case class NUM(s:String) extends Tok
  case class OP(s:String) extends Tok
  case class LP() extends Tok { val s="(" }
  case class RP() extends Tok { val s=")" }

  private val ops2 = Set("==","!=","<>","<=",">=","&&","||")
  private val ops1 = Set("=","<",">","!")
  private val kw = Set("AND","OR","IN","NOT","IS","NULL")

  def parse(expr:String, r:String=>Column): Column = {
    val t = tokenize(expr)
    val p = new Parser(t,r)
    val c = p.expr()
    p.end()
    c
  }

  private def tokenize(s:String): Vector[Tok] = {
    val out = Vector.newBuilder[Tok]
    var i=0
    while(i<s.length){
      s(i) match {
        case c if c.isWhitespace => i+=1
        case '(' => out+=LP(); i+=1
        case ')' => out+=RP(); i+=1
        case '\''|'"' =>
          val q=s(i); i+=1; val st=i
          while(i<s.length && s(i)!=q) i+=1
          out+=STR(s.substring(st,i)); i+=1
        case c if c.isDigit =>
          val st=i; while(i<s.length&&(s(i).isDigit||s(i)=='.')) i+=1
          out+=NUM(s.substring(st,i))
        case c if c.isLetter||c=='_' =>
          val st=i; while(i<s.length&&(s(i).isLetterOrDigit||s(i)=='_'||s(i)=='.')) i+=1
          val v=s.substring(st,i)
          if(kw.contains(v.toUpperCase)) out+=OP(v.toUpperCase) else out+=ID(v)
        case _ =>
          if(i+1<s.length && ops2.contains(s.substring(i,i+2))){
            out+=OP(s.substring(i,i+2)); i+=2
          } else if(ops1.contains(s(i).toString)){
            out+=OP(s(i).toString); i+=1
          } else sys.error(s"Bad char ${s(i)}")
      }
    }
    out.result()
  }

  private class Parser(t:Vector[Tok], r:String=>Column){
    var p=0
    def peek=if(p<t.length)Some(t(p))else None
    def next()={val x=t(p);p+=1;x}
    def end()=if(p!=t.length)sys.error("Trailing tokens")

    def expr():Column = or()
    def or():Column = {var l=and(); while(peek.exists(_.s=="OR")||peek.exists(_.s=="||")){next();l=l.or(and())}; l}
    def and():Column={var l=cmp(); while(peek.exists(_.s=="AND")||peek.exists(_.s=="&&")){next();l=l.and(cmp())}; l}

    def cmp():Column={
      val l=term()
      peek match {
        case Some(OP("="|"==")) if peek2("NULL") => next();next(); l.isNull
        case Some(OP("!="|"<>")) if peek2("NULL") => next();next(); l.isNotNull
        case Some(OP("="|"==")) => next(); l===term()
        case Some(OP("!="|"<>")) => next(); l=!=term()
        case Some(OP("<")) => next(); l < term()
        case Some(OP(">")) => next(); l > term()
        case Some(OP("<=")) => next(); l <= term()
        case Some(OP(">=")) => next(); l >= term()
        case Some(OP("IS")) =>
          next(); val not=peek.exists(_.s=="NOT"); if(not)next(); next()
          if(not)l.isNotNull else l.isNull
        case _ => l
      }
    }

    def term():Column = peek match {
      case Some(ID(v)) => next(); r(v)
      case Some(STR(v)) => next(); lit(v)
      case Some(NUM(v)) => next(); Try(v.toLong).map(lit).getOrElse(lit(v))
      case Some(OP("NULL")) => next(); lit(null)
      case _ => sys.error("Bad term")
    }

    def peek2(v:String)=p+1<t.length && t(p+1).s==v
  }
}

// ============================================================================
// MAPPING ENGINE
// ============================================================================
object MappingEngine {

  private def opt(v:Any)=Option(v).map(_.toString.trim).filter(_.nonEmpty).filterNot(_.equalsIgnoreCase("NULL"))
  private def i(v:Any)=Try(v.toString.toInt).getOrElse(999999)

  private def isValue(s:Option[String])=s.exists(_.equalsIgnoreCase("<VALUE>"))
  private def isNullToken(s:Option[String])=s.exists(_.equalsIgnoreCase("<NULL>"))
  private def isDefault(s:Option[String])=s.exists(_.equalsIgnoreCase("<DEFAULT>"))

  private def resolver(df:DataFrame, defaultTable:Option[String]): String=>Column = {
    val m=df.columns.map(c=>c.toLowerCase->c).toMap
    name=>{
      val t=name.split('.').last.toLowerCase
      m.getOrElse(name.toLowerCase,
        m.getOrElse(t,
          sys.error(s"Unresolved column $name")))
        |> (c=>col(s"`$c`"))
    }
  }

  implicit class Pipe[A](a:A){def |>[B](f:A=>B)=f(a)}

  def applyRules(dest:String, wide:DataFrame, mappingDf:DataFrame, basePk:Seq[String]): DataFrame = {
    val rows = mappingDf.filter($"DestTableName"===dest).collect().toSeq

    val rules = rows.map { r =>
      MapRule(
        opt(r.getAs[Any]("DestColumnName")).get,
        opt(r.getAs[Any]("SourceTableName")),
        opt(r.getAs[Any]("SourceColumnName")),
        opt(r.getAs[Any]("SourceValue")),
        opt(r.getAs[Any]("DestValue")),
        opt(r.getAs[Any]("DestValueDescription")),
        i(r.getAs[Any]("ConversionType")),
        opt(r.getAs[Any]("Condition")),
        i(r.getAs[Any]("Priority"))
      )
    }.groupBy(_.destColumn)

    var out = wide

    rules.foreach { case (dc, rs) =>
      val sorted = rs.sortBy(_.priority)
      val rsv = resolver(out,None)

      def build(sel:MapRule=>Option[String], pass:Boolean)={
        sorted.foldLeft(lit(null):Column){ (acc,r)=>
          val srcOk = (r.sourceColumn,r.sourceValueRaw) match {
            case (Some(c),Some(v)) if isValue(Some(v)) => lit(true)
            case (Some(c),Some(v)) if isNullToken(Some(v)) => lit(true)
            case (Some(c),Some(v)) => rsv(c)===lit(v)
            case (Some(c),None) => rsv(c).isNull
            case _ => lit(true)
          }
          val condOk = r.conditionRaw.map { c =>
            ConditionParser.parse(ConditionSanitizer.sanitize(c), rsv)
          }.getOrElse(lit(true))
          val value =
            if(pass && isValue(sel(r))) r.sourceColumn.map(rsv).getOrElse(lit(null))
            else sel(r).map(lit).getOrElse(lit(null))
          when(acc.isNull && srcOk && condOk, value).otherwise(acc)
        }
      }

      out = out
        .withColumn(dc, build(_.destValueRaw,true))
        .withColumn(s"${dc}Text", build(_.destValueDescRaw,false))
    }

    out.select((basePk ++ rules.keys.flatMap(c=>Seq(c,s"${c}Text"))).distinct.map(col):_*)
  }
}

// ============================================================================
// AUTOMATION (FINAL ENTRY POINT)
// ============================================================================
object MappingAutomation {

  def defaultTableLoader(spark:SparkSession): String=>DataFrame =
    t => spark.table(t)

  def buildAllDestTablesAsMap(
    spark: SparkSession,
    mappingDf: DataFrame,
    pkByTable: Map[String,Seq[String]],
    joinEdges: Seq[JoinEdge],
    sourcePriority: Map[String,Int]=Map.empty
  ): Map[String,DataFrame] = {

    val loader = defaultTableLoader(spark)
    val dests = mappingDf.select("DestTableName").distinct().as[String].collect()

    dests.map { dest =>
      val base = mappingDf.filter($"DestTableName"===dest)
        .select("SourceTableName").as[String].head()

      val df = loader(base)
      val pk = pkByTable(base)

      dest -> MappingEngine.applyRules(dest, df, mappingDf, pk)
    }.toMap
  }
}

// ============================================================================
// USAGE
// ============================================================================
// val result: Map[String,DataFrame] = MappingAutomation.buildAllDestTablesAsMap(
//   spark,
//   mappingDf,
//   pkByTable,
//   joinEdges,
//   sourceTablePriority
// )
//
// result("Vehicle").show(false)
// result("Person").show(false)
