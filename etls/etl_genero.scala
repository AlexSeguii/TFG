%spark

// Archivo: etl_genero.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


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
