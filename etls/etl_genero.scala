%spark

// Archivo: etl_genero.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



// UDF para normalizar cadenas: pasar a minúsculas y quitar acentos
import java.text.Normalizer
val normalizeUDF = udf { s: String =>
  if (s == null) null
  else {
    Normalizer.normalize(s.toLowerCase, Normalizer.Form.NFD)
      .replaceAll("\\p{M}", "")
      .trim
  }
}

// Cliente de Google Cloud Translate (modelo NMT)
import com.google.cloud.translate.{Translate, TranslateOptions, Translation}
val translateClient: Translate = TranslateOptions.getDefaultInstance.getService

val translateUDF = udf { text: String =>
  if (text == null || text.trim.isEmpty) "Unknown"
  else {
    val tr: Translation = translateClient.translate(
      text,
      Translate.TranslateOption.targetLanguage("en"),
      Translate.TranslateOption.model("nmt")
    )
    tr.getTranslatedText
  }
}

// UDF para convertir a mayuscula la primera letra de cada palabra
val properCaseUDF = udf { s: String =>
  if (s == null) null
  else {
    s.split("\\s+")
     .map(w => w.head.toUpper + w.tail.toLowerCase)
     .mkString(" ")
     .trim
  }
}



// Seleccionar la base de datos en Hive
spark.sql("USE mydb")

// Definir la ruta del archivo
val filePath = "file:///home/asa117/Descargas/zzz_ficheros_ETLS/title.basics.tsv"

// Cargar el fichero TSV
val df = spark.read
  .option("header", "true")
  .option("delimiter", "\t")
  .option("inferSchema", "true")
  .csv(filePath)

// Dividir el campo 'genres' por comas y explotar en filas individuales
val explodedDF = df.selectExpr("explode(split(genres, ',')) as genero")

// Eliminar espacios en blanco, valores nulos y duplicados
val cleanedDF = explodedDF.withColumn("genero", trim(col("genero")))
  .filter(col("genero").isNotNull && col("genero") =!= "\\N")
  .dropDuplicates()

// Crear el DF con "Sin genero" y asignarle ID 1 (asegurando que id_genero sea INT)
val sinGeneroDF = spark.createDataFrame(Seq((1, "Sin genero"))).toDF("id_genero", "genero")

// Numerar los otros géneros empezando desde 2 (y asegurando que id_genero sea INT)
val windowSpec = Window.orderBy("genero")
val generosDF = cleanedDF.withColumn("id_genero", (row_number().over(windowSpec) + 1))

// Unir ambos DataFrames asegurando el mismo tipo de datos y el orden correcto
val resultDF = sinGeneroDF.union(generosDF.select("id_genero", "genero"))
  .orderBy("id_genero") // Asegurar que el orden sea correcto

// Imprimir en pantalla para revisar
resultDF.show(50, false)

// Guardar en la tabla Hive 'genero'
resultDF.write.mode("overwrite").insertInto("genero")
