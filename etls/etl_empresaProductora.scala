%spark

//ETL empresa productora

//OJO, aqui en el fichero original quite algunos caracteres raros como " "  ' ' . - ,    etc.   (Fijate bien que usas los ficheros correctos)
//Aqui el titlr.basics   y     en local en _webscrapping

// Importar librerías necesarias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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
