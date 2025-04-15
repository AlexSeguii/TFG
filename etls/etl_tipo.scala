%spark

// Seleccionar la base de datos en Hive
spark.sql("USE mydb")

// Crear los datos para la tabla de tipos
val tipos = Seq(
  (1, "pelicula"),
  (2, "serie")
)

// Crear el DataFrame con los tipos
val tiposDF = spark.createDataFrame(tipos).toDF("id_tipo", "tipo")

// Imprimir para revisar
tiposDF.show()

// Guardar los datos en la tabla Hive 'tipo'
tiposDF.write.mode("overwrite").insertInto("tipo")
