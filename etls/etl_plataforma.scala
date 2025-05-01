// Archivo: etl_plataforma.scala

// Importar las librerías necesarias
import spark.implicits._

// 1. Definir los datos para la tabla 'plataforma'
val plataformaData = Seq(
  (1, "Sin plataforma"),
  (2, "Multiplataforma"),
  (3, "Netflix"),
  (4, "PrimeVideo"),
  (5, "AppleTVPlus")
)

// 2. Convertir la secuencia en un DataFrame, asignando nombres de columnas según el esquema
val dfPlataforma = plataformaData.toDF("id_plataforma", "plataforma")

// 3. Visualizar el DataFrame para verificar los datos
dfPlataforma.show()

// 4. Escribir el DataFrame en la tabla Hive 'mydb.plataforma'
dfPlataforma.write.mode("overwrite").saveAsTable("mydb.plataforma")

//---OPCIONAL---
// 5. Definir la ruta donde quieres guardar el resultado del ETL en formato CSV (puede ser HDFS o sistema local)
// val outputPath = "ruta/indicada/plataforma_etl_output"

// 6. Guardar el DataFrame en un único fichero CSV (con encabezado) en la ruta especificada
//dfPlataforma.coalesce(1)
  //.write.mode("overwrite")
  //.option("header", "true")
  //.csv(outputPath)