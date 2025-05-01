%spark

//ETL Premio mas importante
//Aqui tendremos todos los posibles premios que tienen las peliculas (debemos hacer una logica para ver cual es el mas importante y ese lo pondremos en la tabla de hechos)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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

// 2) Extraer la columna "oscarsWon" y renombrarla a "premio"
val dfPremiosRaw = dfRaw.select(col("oscarsWon").alias("premio"))

// 3) Limpiar el campo: quitar corchetes y comillas simples  
val dfPremiosClean = dfPremiosRaw.withColumn("premio", 
  regexp_replace(regexp_replace(col("premio"), "\\[|\\]", ""), "'", "")
)

// 4) Normalizar el campo: si queda vacío o es "Desconocido", se asigna "Sin premio"
val dfPremiosNorm = dfPremiosClean.withColumn("premio",
  when(trim(col("premio")) === "" || trim(col("premio")) === "Desconocido", "Sin premio")
    .otherwise(trim(col("premio")))
)

// 5) Dividir por comas en caso de tener más de un premio y explotar en filas individuales
val dfPremiosExploded = dfPremiosNorm.withColumn("premio", explode(split(col("premio"), ",")))

// 6) Quitar espacios y eliminar duplicados
val dfPremiosFinal = dfPremiosExploded.withColumn("premio", trim(col("premio")))
  .dropDuplicates()

// 7) Asegurarnos de incluir la fila "Sin premio" (si no está) y unir con los demás premios
val sinPremioDF = spark.createDataset(Seq("Sin premio")).toDF("premio")
val dfPremiosUnion = sinPremioDF.union(dfPremiosFinal).dropDuplicates()

// 8) Crear columna auxiliar para el orden: "Sin premio" tendrá valor 0, y otros 1
val dfConOrden = dfPremiosUnion.withColumn("orden", when(col("premio") === "Sin premio", 0).otherwise(1))

// 9) Asignar id_premioMasImportante incremental usando la columna auxiliar para asegurarnos que "Sin premio" quede primero
val windowSpec = Window.orderBy(col("orden"), col("premio"))
val dfPremiosConID = dfConOrden.withColumn("id_premioMasImportante", row_number().over(windowSpec))
  .select("id_premioMasImportante", "premio")

// 10) Mostrar el resultado para revisión
dfPremiosConID.show(50, false)

// 11) Insertar en la tabla Hive "premioMasImportante"
dfPremiosConID.write.mode("overwrite").insertInto("premioMasImportante")
