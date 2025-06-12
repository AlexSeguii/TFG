%spark

// ETL LANZAMIENTO – GENERACIÓN DE FECHAS

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Seleccionar la base de datos en Hive
spark.sql("USE mydb")

// -----
// 1) Generar todas las fechas mensuales entre 1984-01-01 y 2031-12-01
//    Usamos sequence para crear un array de fechas con paso de 1 mes y luego explode.
val dfFechasGen = spark.sql(
  """
  SELECT explode(
    sequence(
      to_date('1984-01-01'),
      to_date('2031-12-01'),
      interval 1 month
    )
  ) AS fecha
  """
)

// 2) Extraer mes y año
val dfMesAnyo = dfFechasGen
  .withColumn("mes", month(col("fecha")))
  .withColumn("anyo", year(col("fecha")))

// 3) Calcular la época según el mes
val dfConEpoca = dfMesAnyo.withColumn(
  "epoca",
  when(col("mes") === 12,    "Navidad")
   .when(col("mes").isin(6,7,8),   "Verano")
   .when(col("mes").isin(3,4,5),   "Primavera")
   .when(col("mes").isin(9,10,11), "Otoño")
   .when(col("mes").isin(1,2),     "Invierno")
   .otherwise("Sin epoca")  // aunque en rango 1–12 nunca entraría aquí
)
// -----

// 4) Añadir la fila "Desconocido"
val dfDesconocido = spark.createDataFrame(Seq(
  (null.asInstanceOf[Integer], "Desconocido", null.asInstanceOf[Integer])
)).toDF("mes","epoca","anyo")

// 5) Unir, eliminar duplicados y ordenar
val dfSelect = dfConEpoca
  .select("mes","epoca","anyo")
  .union(dfDesconocido)
  .dropDuplicates()

// 6) Asignar id_lanzamiento incremental
val windowSpec = Window.orderBy(col("anyo"), col("mes"))
val dfConID = dfSelect
  .withColumn("id_lanzamiento", row_number().over(windowSpec))
  .select("id_lanzamiento","mes","epoca","anyo")

// 7) Mostrar para revisión
dfConID.show(50,false)

// 8) Guardar en Hive
dfConID.write.mode("overwrite").insertInto("lanzamiento")