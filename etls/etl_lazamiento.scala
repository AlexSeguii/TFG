%spark

//ETL LANZAMIENTO

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Definir un objeto serializable para parsear la fecha
object DateUtils extends Serializable {
  def parseFecha(fecha: String): (Integer, Integer) = {
    if (fecha == null || fecha == "Desconocido") {
      (null, null)
    } else {
      // Formato esperado: "dia/mes/anyo"
      val parts = fecha.split("/")
      if (parts.length == 3) {
        try {
          val mes = parts(1).toInt
          val anyo = parts(2).toInt
          (mes, anyo)
        } catch {
          case _: Throwable => (null, null)
        }
      } else {
        (null, null)
      }
    }
  }
}

// Seleccionar la base de datos en Hive
spark.sql("USE mydb")

// Definir la ruta del fichero (ajusta según corresponda)
val filePath = "file:///home/asa117/Descargas/zzz_ficheros_ETLS/title.basics.tsv"

// 1) Leer el fichero TSV
val dfRaw = spark.read
  .option("header", "true")
  .option("delimiter", "\t")
  .option("inferSchema", "true")
  .csv(filePath)

// 2) Extraer y normalizar la columna releaseDate:
//    Si es null, vacío o "Desconocido", lo forzamos a "Desconocido"
val dfFechas = dfRaw.select(
  when(col("releaseDate").isNull || col("releaseDate") === "" || col("releaseDate") === "Desconocido", lit("Desconocido"))
    .otherwise(col("releaseDate"))
    .alias("releaseDate")
)

// 3) Crear la UDF de parseo usando el objeto serializable DateUtils
val parseFechaUDF = udf((fecha: String) => DateUtils.parseFecha(fecha))

// 4) Aplicar la UDF para obtener mes y anyo
val dfParsed = dfFechas.withColumn("parsed", parseFechaUDF(col("releaseDate")))
val dfMesAnyo = dfParsed.withColumn("mes", col("parsed._1"))
  .withColumn("anyo", col("parsed._2"))
  .drop("parsed")

// 5) Definir la columna "epoca":
//    Si releaseDate es "Desconocido" -> "Desconocido"
//    Si mes==12 -> "Navidad"
//    Si mes es 6,7,8 -> "Verano"
//    Si mes es 3,4,5 -> "Primavera"
//    Si mes es 9,10,11 -> "Otoño"
//    Si mes es 1 o 2 -> "Invierno"
//    Sino -> "Sin epoca"
val dfConEpoca = dfMesAnyo.withColumn(
  "epoca",
  when(col("releaseDate") === "Desconocido", "Desconocido")
   .when(col("mes") === 12, "Navidad")
   .when(col("mes").isin(6,7,8), "Verano")
   .when(col("mes").isin(3,4,5), "Primavera")
   .when(col("mes").isin(9,10,11), "Otoño")
   .when(col("mes").isin(1,2), "Invierno")
   .otherwise("Sin epoca")
)

// 6) Seleccionar solo las columnas (mes, epoca, anyo) y eliminar duplicados
val dfSelect = dfConEpoca.select("mes", "epoca", "anyo").dropDuplicates()

// 7) Crear (si es que no existe) o incluir la fila "Desconocido" (mes=null, anyo=null, epoca="Desconocido")
val dfDesconocido = spark.createDataFrame(Seq((null, "Desconocido", null))).toDF("mes", "epoca", "anyo")
val dfFinal = dfSelect.union(dfDesconocido).dropDuplicates()

// 8) Asignar un id_lanzamiento incremental usando una ventana creada inline para evitar capturar variables externas
val dfConID = dfFinal.withColumn("id_lanzamiento", row_number().over(Window.orderBy("mes", "anyo", "epoca")))
  .select("id_lanzamiento", "mes", "epoca", "anyo")

// 9) Mostrar el resultado para revisión
dfConID.show(50, false)

// 10) Insertar en la tabla Hive "lanzamiento"
dfConID.write.mode("overwrite").insertInto("lanzamiento")
