%spark

//ETL IDIOMA

//NOTA IMPORTANTE : Hay contenidos que solo aparece el idioma original (en IMDB solo aprece la VO)
//Los trataresmos como contenidos con un unico lenguaje (su VO)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// 0) Use la base de datos Hive
spark.sql("USE mydb")

// 1) Leer el fichero TSV
val filePath = "file:///home/asa117/Descargas/zzz_ficheros_ETLS/title.basics.tsv"
val dfRaw = spark.read
  .option("header","true")
  .option("delimiter","\t")
  .option("inferSchema","true")
  .csv(filePath)

// 2) Extraer y normalizar la columna "languages"
// [CAMBIO]   - null/""/"Desconocido" => "Desconocido"
//             - trim para limpiar espacios
val dfLang = dfRaw.select(
  when(col("languages").isNull || trim(col("languages")) === "" || trim(col("languages")) === "Desconocido",
       "Desconocido")
    .otherwise(trim(col("languages")))
    .alias("langs")
)

// 3) Clasificar cada fila en categoría única:
//    - "multilenguaje" si tiene >1 idioma tras split
//    - "Desconocido" si es ese literal
//    - else, el único idioma
// [CAMBIO] explode no, usamos map directo
val dfCat = dfLang.withColumn("categoria",
  when(col("langs") === "Desconocido", lit("Desconocido"))
   .when(size(split(col("langs"),",")) > 1, lit("Multilenguaje"))
   .otherwise(trim(col("langs")))
)

// 4) Obtener todas las categorías únicas
val dfDistinct = dfCat.select("categoria").dropDuplicates()

// 5) Preparar el orden: multilenguaje=0, Desconocido=1, resto=2
val dfWithOrder = dfDistinct.withColumn("orden",
  when(col("categoria") === "Multilenguaje", 0)   // <<< multilenguaje = 0
   .when(col("categoria") === "Desconocido",    1) // <<< Desconocido = 1
   .otherwise(2)                                 // <<< resto = 2
)

// 6) Asignar id_idioma con row_number() ordenado por (orden, categoria)
//    Esto garantiza que multilenguaje sea el primer registro (id = 1).
val windowSpec = Window.orderBy(col("orden"), col("categoria"))
val dfWithID = dfWithOrder
  .withColumn("id_idioma", row_number().over(windowSpec))
  .select(
    col("id_idioma"),
    col("categoria").alias("idioma")
  )
// 7) Mostrar para revisión
dfWithID.show(50, false)

// 8) Insertar en la tabla Hive `idioma`
dfWithID.write.mode("overwrite").insertInto("idioma")


