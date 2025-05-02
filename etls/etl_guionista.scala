%spark

//ETL guionista

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// 0) Usar la base de datos Hive
spark.sql("USE mydb")

// 1) Leer name.basics.tsv
val dfRaw = spark.read
  .option("header","true")
  .option("delimiter","\t")
  .option("inferSchema","true")
  .csv("file:///home/asa117/Descargas/zzz_ficheros_ETLS/name.basics.tsv")

// 2) Filtrar solo “writer” en primaryProfession
val dfWriters = dfRaw.filter(
  array_contains(split(col("primaryProfession"), ","), "writer")
)

// 3) Calcular edad y asignar grupo de edad
val dfWithAge = dfWriters
  .withColumn("age", when(col("birthYear").isNull, lit(null).cast("int"))
                     .otherwise(lit(2025) - col("birthYear")))
  .withColumn("grupo_edad",
    when(col("age").isNull, "Desconocido")
     .when(col("age") <= 17, "Menor de edad")
     .when(col("age") <= 30, "Joven")
     .when(col("age") <= 60, "Adulto")
     .otherwise("Adulto mayor")
  )

// 4) Mapear reputación según Awards Count
val dfWithReputation = dfWithAge.withColumn("reputacion",
    when(col("Awards Count") === 0, "Desconocido")
     .when(col("Awards Count") === 1, "Poco conocido")
     .when(col("Awards Count").between(2,3), "Algo conocido")
     .when(col("Awards Count").between(4,10), "Conocido")
     .when(col("Awards Count").between(11,20), "Muy conocido")
     .otherwise("Estrella consagrada")
  )

// 5) Seleccionar columnas (mantener nconst)
val dfClean = dfWithReputation.select(
    col("nconst"),
    col("primaryName").alias("nombre"),
    col("grupo_edad"),
    col("reputacion"),
    trim(col("City of Birth")).alias("ciudad"),
    trim(col("State of Birth")).alias("estado"),
    trim(col("Country of Birth")).alias("pais"),
    col("Won Oscar").cast("boolean").alias("ganador_oscar")
  ).dropDuplicates("nconst")

// 6) Generar surrogate key desplazado +1 (reservar id=1 para “Sin guionista”)
val windowSpec = Window.orderBy(col("nconst"))
val dfRealWriters = dfClean
  .withColumn("id_guionista", row_number().over(windowSpec) + 1)
  .select("id_guionista","nconst","nombre","grupo_edad","reputacion","ciudad","estado","pais","ganador_oscar")

// 7) Crear registro “Sin guionista” con id_guionista=1
val sinWriterDF = Seq(
  (1, "n/a", "Sin guionista", null: String, null: String, null: String, null: String, null: String, null: java.lang.Boolean)
).toDF("id_guionista","nconst","nombre","grupo_edad","reputacion","ciudad","estado","pais","ganador_oscar")

// 8) Unir y escribir con unionByName
val dfDimWriter = sinWriterDF.unionByName(dfRealWriters)
dfDimWriter.show(20, false)

dfDimWriter.write.mode("overwrite").insertInto("guionista")

