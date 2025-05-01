%spark

//ETL ACTOR

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// 0) Usar la base de datos Hive
spark.sql("USE mydb")

// 1) Leer el fichero name.basics.tsv
val filePath = "file:///home/asa117/Descargas/zzz_ficheros_ETLS/name.basics.tsv"
val dfRaw = spark.read
  .option("header", "true")
  .option("delimiter", "\t")
  .option("inferSchema", "true")
  .csv(filePath)

// 2) Filtrar solo “actor” o “actress”
val dfActors = dfRaw.filter(
  array_contains(split(col("primaryProfession"), ","), "actor") ||
  array_contains(split(col("primaryProfession"), ","), "actress")
)

// 3) Calcular edad y grupo de edad
val dfWithAge = dfActors.withColumn("age",
    when(col("birthYear").isNull, lit(null).cast("int"))
     .otherwise(lit(2025) - col("birthYear"))
  ).withColumn("grupo_edad",
    when(col("age").isNull, "Desconocido")
     .when(col("age") <= 17, "Menor de edad")
     .when(col("age") <= 30, "Joven")
     .when(col("age") <= 60, "Adulto")
     .otherwise("Adulto mayor")
  )

// 4) Mapear reputación
val dfWithReputation = dfWithAge.withColumn("reputacion",
    when(col("Awards Count") === 0, "Desconocido")
     .when(col("Awards Count") === 1, "Poco conocido")
     .when(col("Awards Count").between(2,3), "Algo conocido")
     .when(col("Awards Count").between(4,10), "Conocido")
     .when(col("Awards Count").between(11,20), "Muy conocido")
     .otherwise("Estrella consagrada")
  )

// 5) Seleccionar y limpiar columnas, manteniendo nconst
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

// 6) Generar temp ID por orden de nconst
val windowSpec = Window.orderBy(col("nconst"))
val dfRealActors = dfClean
  .withColumn("id_actor", row_number().over(windowSpec) + 1)  // +1 para reservar el 1
  .select("id_actor","nombre","grupo_edad","reputacion","ciudad","estado","pais","ganador_oscar")

// 7) Crear el registro “Sin actor” con unionByName (Spark infiere nulables)
val sinActorDF = Seq(
  (1, "Sin actor", null, null, null, null, null, null)
).toDF("id_actor","nombre","grupo_edad","reputacion","ciudad","estado","pais","ganador_oscar")

// 8) Unir y escribir
val dfDimActor = sinActorDF.unionByName(dfRealActors)
dfDimActor.show(20, false)

dfDimActor.write.mode("overwrite").insertInto("actor")
