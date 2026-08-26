// ---------------- Models ----------------
case class ColSpec(out: String, xmlPath: String = "", dt: DataType = StringType, rowNumber: Boolean = false)
case class AutoIncConf(name: String, partitionBy: Seq[String] = Nil, orderBy: Seq[String] = Nil)
case class TableSpec(
  explodes: Seq[String],
  carry: Seq[ColSpec] = Nil,
  cols: Seq[ColSpec],
  autoInc: Seq[AutoIncConf] = Nil
)

// ---------------- Schema walk / safety utilities ----------------
def dtype(dt: DataType, parts: List[String]): Option[DataType] = (dt, parts) match {
  case (_, Nil) => Some(dt)
  case (s: StructType, h :: t) => s.fields.find(_.name == h).flatMap(f => dtype(f.dataType, t))
  case (ArrayType(e,_), ps)    => dtype(e, ps)
  case _ => None
}

// def hasPath(df: DataFrame, path: String): Boolean =
//   Option(path).exists(p => p.nonEmpty && dtype(df.schema, p.split("\\.").toList).nonEmpty)

def hasPath(df: DataFrame, path: String): Boolean = {
  val exists = Option(path).exists { p =>
    val present = dtype(df.schema, p.split("\\.").toList).nonEmpty
    // println(s"Checking path existence: '$p' -> $present")
    present
  }
  if (!exists) println(s"Path '$path' does not exist in schema")
  exists
}


def scalar(df: DataFrame, full: String): Column = dtype(df.schema, full.split("\\.").toList) match {
  case Some(ArrayType(_, _)) => element_at(col(full), 1)
  case _                     => col(full)
}

def explodeChain(df: DataFrame, paths: Seq[String]): (DataFrame, String) = {
  var cur = df
  var pfx = ""
  paths.zipWithIndex.foreach { case (rp, i) =>
    val full  = if (pfx.isEmpty) rp else s"$pfx$rp"
    val alias = s"row$i"
    val posAlias = s"${alias}_pos"
    val rowNumAlias = s"${alias}_rownum"
    val dtOpt = dtype(cur.schema, full.split("\\.").toList)

    // println(s"Exploding path #$i: $full")
    // println(s"Schema before explode:\n${cur.schema.treeString}")

    cur = dtOpt match {
      case Some(ArrayType(_, _)) => 
        //println(s"Exploding array column $full as $alias")
        cur
          .select((cur.columns.map(col) :+ posexplode_outer(col(full)).as(Seq(posAlias, alias))): _*)
          .withColumn(rowNumAlias, col(posAlias) + lit(1))
      case Some(_: StructType) => 
        //println(s"Selecting struct column $full as $alias")
        cur.withColumn(alias, col(full)).withColumn(rowNumAlias, lit(1))
      case Some(_) => 
     //   println(s"Selecting leaf column $full as $alias")
        cur.withColumn(alias, col(full)).withColumn(rowNumAlias, lit(1))
      case None => 
       // println(s"Path $full not found! Creating empty explode column $alias")
        cur
          .select((cur.columns.map(col) :+ posexplode_outer(array()).as(Seq(posAlias, alias))): _*)
          .withColumn(rowNumAlias, col(posAlias) + lit(1))
    }

   // println(s"Schema after explode/select #$i:\n${cur.schema.treeString}")
    pfx = s"$alias."
  }
  (cur, pfx)
}

// ---------------- Casting helpers ----------------
def sqlType(dt: DataType): String = dt match {
  case IntegerType              => "INT"
  case LongType                 => "BIGINT"
  case ShortType                => "SMALLINT"
  case ByteType                 => "TINYINT"
  case DoubleType               => "DOUBLE"
  case FloatType                => "FLOAT"
  case BooleanType              => "BOOLEAN"
  case TimestampType            => "TIMESTAMP"
  case DateType                 => "DATE"
  case d: DecimalType           => s"DECIMAL(${d.precision},${d.scale})"
  case _                        => "STRING"
}

def sqlPath(path: String): String =
  path.split("\\.").map(part => s"`${part.replace("`", "``")}`").mkString(".")

def pick(df: DataFrame, pfx: String, c: ColSpec): Column = {
  if (c.rowNumber) {
    val rowNumPath = if (pfx.nonEmpty) s"${pfx.stripSuffix(".")}_rownum" else c.xmlPath
    val chosen = if (hasPath(df, rowNumPath)) rowNumPath else c.xmlPath

    if (!hasPath(df, chosen)) return lit(null).cast(c.dt).as(c.out)
    val rowNumExpr = sqlPath(chosen)
    return if (c.dt == StringType) expr(rowNumExpr).as(c.out)
           else expr(s"try_cast($rowNumExpr AS ${sqlType(c.dt)})").as(c.out)
  }

  val isAbs = c.xmlPath.startsWith("^")
  val rel = if (isAbs) c.xmlPath.drop(1) else c.xmlPath
  val pref = if (!isAbs && pfx.nonEmpty) s"$pfx$rel" else rel
  val chosen = if (hasPath(df, pref)) pref else if (hasPath(df, rel)) rel else null

  //println(s"Picking '${c.out}' using paths prefixed: '$pref', fallback: '$rel', chosen: $chosen")

  if (chosen == null) {
    //println(s"Column '${c.out}' resolved as null")
    return lit(null).cast(c.dt).as(c.out)
  }

  val isArrayLeaf = dtype(df.schema, chosen.split("\\.").toList).exists(_.isInstanceOf[ArrayType])
  val chosenExpr = sqlPath(chosen)
  val baseExpr = if (isArrayLeaf) s"element_at($chosenExpr, 1)" else chosenExpr

  val colExpr = if (c.dt == StringType) expr(baseExpr).as(c.out)
                else expr(s"try_cast($baseExpr AS ${sqlType(c.dt)})").as(c.out)

  //println(s"Expression for '${c.out}': $colExpr")
  colExpr
}

// ---------------- Table builder with debug ----------------
def makeTable(root: DataFrame, spec: TableSpec): DataFrame = {
  println(s"Creating table with explodes: ${spec.explodes.mkString(",")}")
  val (base, pfx) = if (spec.explodes.isEmpty) (root, "") else explodeChain(root, spec.explodes)
  //println(s"Final prefix after explodeChain: '$pfx'")
  val colsExpr = (spec.carry ++ spec.cols).map(c => pick(base, pfx, c))
  //println(s"Selecting columns: ${colsExpr.mkString(",")}")
  spec.autoInc.foldLeft(base.select(colsExpr:_*)) { (df, cfg) =>
    addAutoInc(df, cfg)
  }
}

// ---------------- Utility functions ----------------
def firstExisting(df: DataFrame, candidates: Seq[String]): Option[String] =
  candidates.find(p => hasPath(df, p))

def addAutoInc(df: DataFrame, cfg: AutoIncConf): DataFrame = {
  val base = if (cfg.orderBy.nonEmpty) df else df.withColumn("_miid", monotonically_increasing_id())
  val orderCols = if (cfg.orderBy.nonEmpty) cfg.orderBy.map(col) else Seq(col("_miid"))
  val w = if (cfg.partitionBy.nonEmpty)
    Window.partitionBy(cfg.partitionBy.map(col): _*).orderBy(orderCols: _*)
  else
    Window.orderBy(orderCols: _*)
  val withId = base.withColumn(cfg.name, dense_rank().over(w))
  if (cfg.orderBy.nonEmpty) withId else withId.drop("_miid")
}

//print(PropertyDamagePath)
