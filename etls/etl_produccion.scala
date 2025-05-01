%spark

//ETL produccion
//pais en el que se hizo la pelicula/serie


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

// 2) Extraer la columna "country" y renombrarla a "pais"
// [CAMBIO] Normalizamos: si es null, vacío o ya es "Desconocido", se asigna "Desconocido".
val dfPais = dfRaw.select(
  when(col("country").isNull || trim(col("country")) === "" || trim(col("country")) === "Desconocido", "Desconocido")
    .otherwise(trim(col("country")))
    .alias("pais")
)

// 3) Eliminar duplicados
val dfUnique = dfPais.dropDuplicates()

// 4) Asegurarnos de incluir "Desconocido" (aunque si ya existe, quedará un único registro)
// [CAMBIO] Se crea un DataFrame con "Desconocido" y se hace unión
val sinPaisDF = spark.createDataset(Seq("Desconocido")).toDF("pais")
val dfUnion = sinPaisDF.union(dfUnique).dropDuplicates()

// 5) Asignar un id_produccion incremental y garantizar que "Desconocido" quede primero.
// [CAMBIO] Se agrega una columna auxiliar "orden": "Desconocido" = 0; para los demás, 1.
val dfConOrden = dfUnion.withColumn("orden", when(col("pais") === "Desconocido", 0).otherwise(1))
val windowSpec = Window.orderBy(col("orden"), col("pais"))
val dfConID = dfConOrden.withColumn("id_produccion", row_number().over(windowSpec))
  .select("id_produccion", "pais")

// 6) Mostrar resultado para verificar
dfConID.show(50, false)

// 7) Insertar en la tabla Hive "produccion"
dfConID.write.mode("overwrite").insertInto("produccion")
