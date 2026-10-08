import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.pdfbox.multipdf.PDFMergerUtility
import org.apache.pdfbox.io.MemoryUsageSetting

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64


// --------------------------------------------------
// Merge 2 Base64-encoded PDFs
// --------------------------------------------------
def mergeBase64Pdf(pdf1: String, pdf2: String): String = {

  if (pdf1 == null || pdf2 == null) {
    null
  } else {

    // Handles both normal Base64 and strings like:
    // data:application/pdf;base64,JVBERi0x...
    def cleanBase64(value: String): String = {
      if (value.contains(",")) value.substring(value.indexOf(",") + 1)
      else value
    }

    val decoder = Base64.getMimeDecoder

    val pdf1Bytes = decoder.decode(cleanBase64(pdf1))
    val pdf2Bytes = decoder.decode(cleanBase64(pdf2))

    val output = new ByteArrayOutputStream()

    val merger = new PDFMergerUtility()

    merger.addSource(
      new ByteArrayInputStream(pdf1Bytes)
    )

    merger.addSource(
      new ByteArrayInputStream(pdf2Bytes)
    )

    merger.setDestinationStream(output)

    merger.mergeDocuments(
      MemoryUsageSetting.setupMainMemoryOnly()
    )

    Base64.getEncoder.encodeToString(
      output.toByteArray
    )
  }
}


// Create Spark UDF
val mergePdfUdf = udf(
  (pdf1: String, pdf2: String) => mergeBase64Pdf(pdf1, pdf2)
)


// --------------------------------------------------
// Merge only CrashIds existing in BOTH DataFrames
// --------------------------------------------------
val mergedPdfDF = df1.alias("a")
  .join(
    df2.alias("b"),
    col("a.CrashId") === col("b.CrashId"),
    "inner"
  )
  .filter(
    col("a.imagedata").isNotNull &&
    col("b.imagedata").isNotNull
  )
  .select(
    col("a.CrashId").as("CrashId"),

    mergePdfUdf(
      col("a.imagedata"),
      col("b.imagedata")
    ).as("imagedata")
  )

//----------------------------------------------------------

import org.apache.spark.sql.functions._
import org.apache.pdfbox.multipdf.PDFMergerUtility
import org.apache.pdfbox.io.MemoryUsageSetting

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64


val mergePdfUdf = udf { (pdf1: String, pdf2: String) =>

  if (pdf1 == null || pdf2 == null) {

    null

  } else {

    // Remove optional data:application/pdf;base64, prefix
    val clean1 =
      if (pdf1.contains(",")) pdf1.substring(pdf1.indexOf(",") + 1)
      else pdf1

    val clean2 =
      if (pdf2.contains(",")) pdf2.substring(pdf2.indexOf(",") + 1)
      else pdf2

    val decoder = Base64.getMimeDecoder

    val bytes1 = decoder.decode(clean1)
    val bytes2 = decoder.decode(clean2)

    val input1 = new ByteArrayInputStream(bytes1)
    val input2 = new ByteArrayInputStream(bytes2)

    val output = new ByteArrayOutputStream()

    try {

      val merger = new PDFMergerUtility()

      merger.addSource(input1)
      merger.addSource(input2)

      merger.setDestinationStream(output)

      merger.mergeDocuments(
        MemoryUsageSetting.setupMainMemoryOnly()
      )

      Base64.getEncoder.encodeToString(
        output.toByteArray
      )

    } finally {

      input1.close()
      input2.close()
      output.close()

    }
  }
}
