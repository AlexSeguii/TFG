%spark

//ETL empresa productora

//OJO, aqui en el fichero original quite algunos caracteres raros como " "  ' ' . - ,    etc.   (Fijate bien que usas los ficheros correctos)
//Aqui el titlr.basics   y     en local en _webscrapping

// Importar librerías necesarias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


// -----
// Normalizar: quitar acentos, pasar a minúsculas y eliminar todo lo que no sea letra o número
import java.text.Normalizer
val normalizeUDF = udf { s: String =>
  if (s == null) null
  else {
    // descomponer acentos, quitar marcas, pasar a lower y eliminar no-alfanuméricos
    Normalizer.normalize(s.toLowerCase, Normalizer.Form.NFD)
      .replaceAll("\\p{M}", "")
      .replaceAll("[^a-z0-9]", "")
      .trim
  }
}


// Seleccionar la base de datos en Hive
spark.sql("USE mydb")

// Definir la ruta del archivo TSV
val filePath = "file:///home/asa117/Descargas/zzz_ficheros_ETLS/title.basics.tsv"

// Cargar el fichero TSV
val df = spark.read
  .option("header", "true")
  .option("delimiter", "\t")
  .option("inferSchema", "true")
  .csv(filePath)

// Procesar el campo 'productionCompany'
// Si es nulo, vacío o contiene "Desconocido" (sin importar mayúsculas/minúsculas), se reemplaza por "Desconocido"
val dfEmpresas = df.select(col("productionCompany"))
  .withColumn("empresaProductora", when(
    col("productionCompany").isNull || trim(col("productionCompany")) === "" || lower(trim(col("productionCompany"))) === "desconocido",
    lit("Desconocido")
  ).otherwise(col("productionCompany")))
  .select("empresaProductora")
  .dropDuplicates()


// Agrupar por la forma “normalizada” y quedarnos con la variante de menor longitud
val dfNorm = dfEmpresas
  .withColumn("norm", normalizeUDF(col("empresaProductora")))

val windowNorm = Window
  .partitionBy(col("norm"))
  .orderBy(length(col("empresaProductora")).asc)

val dfUnicas = dfNorm
  .withColumn("rn", row_number().over(windowNorm))
  .filter(col("rn") === 1)
  .drop("norm", "rn")


// Ordenar de forma especial: "Desconocido" siempre en primer lugar y luego el resto en orden alfabético
val dfOrdenado = dfEmpresas.orderBy(
  when(col("empresaProductora") === "Desconocido", lit(0)).otherwise(lit(1)),
  col("empresaProductora").asc
)

// Asignar el ID secuencial utilizando la misma expresión de ordenación en la ventana
val windowSpec = Window.orderBy(
  when(col("empresaProductora") === "Desconocido", lit(0)).otherwise(lit(1)),
  col("empresaProductora").asc
)
val dfConID = dfOrdenado.withColumn("id_empresaProductora", row_number().over(windowSpec))

// Seleccionar columnas en el orden deseado
val resultDF = dfConID.select("id_empresaProductora", "empresaProductora")
  .orderBy("id_empresaProductora") // Confirmar el orden

// Mostrar en pantalla para revisar
resultDF.show(50, false)

// Guardar el DataFrame en la tabla Hive 'empresaProductora'
resultDF.write.mode("overwrite").insertInto("empresaProductora")
