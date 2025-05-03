%spark

//Tabla hechos contenido Audiovisual

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.expressions.Window

// ----------------------------------------------------------------------------
// 0) Inicializar Spark y usar la base de datos Hive
// ----------------------------------------------------------------------------
val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
spark.sql("USE mydb")

// ----------------------------------------------------------------------------
// 1) Diccionario completo de tasas de cambio (1 unidad → EUR)
// ----------------------------------------------------------------------------
val exchangeRates = Map(
  "$"   -> 0.92,     "A$"  -> 0.61,     "DEM" -> 0.51129,    "FRF" -> 0.152449,
  "ITL" -> 0.0005165,"ROL" -> 0.0000200916,"SEK"-> 0.092,     "£"   -> 1.17,
  "NLG" -> 0.45378,  "CA$" -> 0.68,     "NOK" -> 0.087,      "PTE" -> 0.005,
  "¥"   -> 0.0062,   "FIM" -> 0.16819,  "PLN" -> 0.22,       "CHF" -> 1.02,
  "₹"   -> 0.011,    "ESP" -> 0.006015, "IDR" -> 0.000058,   "DKK" -> 0.134,
  "RON" -> 0.20,     "RUR" -> 0.008534, "₩"   -> 0.00073,    "BEF" -> 0.02479,
  "XAU" -> 2878.32,  "HK$" -> 0.12,     "NZ$" -> 0.57,       "CN¥" -> 0.13,
  "€"   -> 1.00,     "PYG" -> 0.00012,  "ISK" -> 0.0065,     "MX$" -> 0.049,
  "IEP" -> 1.27,     "CZK" -> 0.041,    "SGD" -> 0.68,       "TRL" -> 0.000000606,
  "DOP" -> 0.016,    "HRK" -> 0.13,     "SIT" -> 0.0042,     "₱"   -> 0.017,
  "HUF" -> 0.0027,   "GRD" -> 0.00294,  "JMD" -> 0.0062,     "R$"   -> 0.19,
  "BDT" -> 0.0097,   "YUM" -> 0.01141,  "ARS" -> 0.0055,     "PKR"  -> 0.0051,
  "MYR" -> 0.21,     "ATS" -> 0.0727,   "GEL" -> 0.31,       "BND"  -> 0.68,
  "NPR" -> 0.0073,   "UAH" -> 0.025,    "EGP" -> 0.029,      "THB"  -> 0.026,
  "ZAR" -> 0.051,    "EEK" -> 0.0639,   "IRR" -> 0.000021,   "CLP"  -> 0.0011,
  "BGL" -> 0.0005112,"VEB"-> 0.0101599, "MUR" -> 0.022,      "MAD"  -> 0.091,
  "NT$" -> 0.029,    "₪"   -> 0.26,     "SKK" -> 0.0294,     "NGN"  -> 0.0022,
  "LTL" -> 0.29,     "MTL" -> 2.33,     "LVL" -> 1.42,       "EC$"  -> 0.31,
  "MMK" -> 0.00045,  "₫"   -> 0.000039, "COP" -> 0.00022,    "LUF"  -> 0.02479,
  "CUP" -> 0.036,    "OMR" -> 2.40,     "LKR" -> 0.0043,     "GTQ"  -> 0.11,
  "NAD" -> 0.051,    "MVR" -> 0.049,    "MOP" -> 0.10,       "CRC"  -> 0.0015,
  "TTD" -> 0.13,     "BSD" -> 0.92,     "BOB" -> 0.12,       "KZT"  -> 0.0021,
  "JOD" -> 1.30,     "KES" -> 0.0074,   "SYP" -> 0.00037,    "PEN"  -> 0.25,
  "BHD" -> 2.45,     "GHC" -> 0.058,    "AFA" -> 0.000124813,"QAR"  -> 0.25,
  "AED" -> 0.25,     "BYR" -> 0.000026949,"DZD"-> 0.007,      "AMD"  -> 0.0021,
  "PGK" -> 0.25,     "ANG" -> 0.51,     "BAM" -> 0.51,       "SDD"  -> 0.0000146986,
  "AZM" -> 0.0001042548,"MNT"->0.00028, "KWD" -> 2.77,       "KGS"  -> 0.011,
  "BTN" -> 0.011,    "MKD" -> 0.016,    "ALL" -> 0.0086,     "UYU"  -> 0.024,
  "BIF" -> 0.00042,  "SAR" -> 0.25,     "HNL" -> 0.037,      "UGX"  -> 0.00023,
  "TND" -> 0.30,     "ZWD" -> 0.000002, "MDL" -> 0.046,      "FJD"  -> 0.41,
  "YER" -> 0.0035,   "RWF" -> 0.00082,  "ETB" -> 0.020,      "KHR"  -> 0.00022,
  "FCFA"-> 0.001524, "IQD"-> 0.00063,   "TZS" -> 0.00038,    "LBP"  -> 0.000057,
  "AOA" -> 0.00055,  "PAB" -> 0.92,     "TJS" -> 0.084,      "SLL"  -> 0.000049,
  "ZMK" -> 0.0000313672,"CVE"->0.0091,  "BBD" -> 0.46,       "CDF"  -> 0.00041,
  "TMM" -> 0.0000502434,"SOS"->0.00080, "KPW" -> 0.00010,    "SZL"  -> 0.051,
  "AWG" -> 0.51,     "UZS" -> 0.000055, "CFPF"->0.0084,      "LYD"  -> 0.65,
  "BMD" -> 0.92
)

// ----------------------------------------------------------------------------
// 2) UDF para convertir un string monetario a EUR (Long) o null
// ----------------------------------------------------------------------------
val toEur = udf { raw: String =>
  if (raw == null || raw.trim.isEmpty || raw == "Desconocido") null: java.lang.Long
  else {
    val clean = raw.replace("\u202f"," ").trim
    val curOpt = """^\s*([^\d\(]+)""".r.findFirstMatchIn(clean)
                   .map(_.group(1).replaceAll("\\(.*\\)","").trim)
    val numOpt = """([\d\.,]+)""".r.findFirstMatchIn(clean)
                   .map(_.group(1)
                            .replaceAll("""[.,](?=\d{3}\b)""","")
                            .replace(',','.'))
    (curOpt,numOpt) match {
      case (Some(cur),Some(num)) =>
        exchangeRates.get(cur) match {
          case Some(rate) =>
            try java.lang.Long.valueOf(Math.round(num.toDouble*rate))
            catch{case _:Throwable=> null}
          case None => null
        }
      case _ => null
    }
  }
}

// ----------------------------------------------------------------------------
// 3) Leer ficheros orígen y dimensiones
// ----------------------------------------------------------------------------
val dfBasics  = spark.read.option("header","true").option("delimiter","\t")
                    .csv("file:///home/asa117/Descargas/zzz_ficheros_ETLS/title.basics.tsv")
val dfRatings = spark.read.option("header","true").option("delimiter","\t")
                    .csv("file:///home/asa117/Descargas/zzz_ficheros_ETLS/title.ratings.tsv")
val dfNetflix = spark.read.option("header","true")
                    .csv("file:///home/asa117/Descargas/zzz_ficheros_ETLS/netflix.csv")
val dfPrime   = spark.read.option("header","true")
                    .csv("file:///home/asa117/Descargas/zzz_ficheros_ETLS/primevideo.csv")
val dfAppleTV = spark.read.option("header","true")
                    .csv("file:///home/asa117/Descargas/zzz_ficheros_ETLS/appletv.csv")

val dfGuion = spark.table("guionista")
                  .select(col("nconst").as("g_nconst"), col("id_guionista"))
val dfDir   = spark.table("director")
                  .select(col("nconst").as("d_nconst"), col("id_director"))
val dfAct   = spark.table("actor")
                  .select(col("nconst").as("a_nconst"), col("id_actor"))
val dfProd  = spark.table("produccion")
                  .select(col("pais").as("prod_pais"), col("id_produccion").as("dim_id_produccion"))
val dfEmp   = spark.table("empresaProductora")
                  .select(col("empresaProductora").as("emp_name"), col("id_empresaProductora"))
val dfLan   = spark.table("lanzamiento")
                  .select(col("mes").as("lan_mes"),
                          col("epoca").as("lan_epoca"),
                          col("anyo").as("lan_anyo"),
                          col("id_lanzamiento"))
val dfLang  = spark.table("idioma")
                  .select(col("idioma").as("lang_str"), col("id_idioma"))
val dfGen   = spark.table("genero")
                  .select(col("genero").as("gen_str"), col("id_genero"))

// flags de plataforma
val netflixSet = dfNetflix.select("tconst").distinct.withColumn("isNetflix", lit(1))
val primeSet   = dfPrime.select("tconst").distinct  .withColumn("isPrime",   lit(1))
val appleSet   = dfAppleTV.select("tconst").distinct.withColumn("isApple",   lit(1))

// ----------------------------------------------------------------------------
// 4) Extraer cast, géneros, fecha y awards array
// ----------------------------------------------------------------------------
val dfWithExtras = dfBasics
  // cast para separarar las ,
  .withColumn("cast_arr", split(col("cast"),","))  
  .withColumn("actor1", trim(col("cast_arr").getItem(0)))
  .withColumn("actor2", trim(col("cast_arr").getItem(1)))
  .withColumn("actor3", trim(col("cast_arr").getItem(2)))
  // géneros
  .withColumn("gen_arr", split(col("genres"),","))  
  .withColumn("g1", trim(col("gen_arr").getItem(0)))
  .withColumn("g2", trim(col("gen_arr").getItem(1)))
  .withColumn("g3", trim(col("gen_arr").getItem(2)))
  // mes/año
  .withColumn("mes",
     when(col("releaseDate").isNull || col("releaseDate")==="" || col("releaseDate")==="Desconocido",
          lit(null).cast("int"))
     .otherwise(split(col("releaseDate"),"/").getItem(1).cast("int"))
  )
  .withColumn("anyo",
     when(col("releaseDate").isNull || col("releaseDate")==="" || col("releaseDate")==="Desconocido",
          lit(null).cast("int"))
     .otherwise(split(col("releaseDate"),"/").getItem(2).cast("int"))
  )
  // raw languages
  .withColumn("raw_lang", trim(col("languages")))
  // awards → quitar [ ] ' y luego split+trim
  .withColumn("award_arr",
    expr("""
      transform(
        split(
          regexp_replace(oscarsWon, "[\\[\\]']", ""),
          ","
        ),
        x -> trim(x)
      )
    """)
  )

// ----------------------------------------------------------------------------
// 5) Determinar “premio más importante” por prioridad
// ----------------------------------------------------------------------------
val orderedAwards = Seq(
  "Best Picture",
  "Best Director",
  "Best Actor in a Leading Role",
  "Best Actress in a Leading Role",
  "Best Original Screenplay",
  "Best Adapted Screenplay",
  "Best Actor in a Supporting Role",
  "Best Actress in a Supporting Role",
  "Best Cinematography",
  "Best Film Editing",
  "Best Art Direction",
  "Best Costume Design",
  "Best Makeup and Hairstyling",
  "Best Visual Effects",
  "Best Sound Mixing",
  "Best Sound Editing",
  "Best Sound",
  "Best Music (Original Score)",
  "Best Original Song",
  "Best Foreign Language Film",
  "Best Animated Feature",
  "Best Documentary",
  "Best Dance Direction",
  "Best Assistant Director",
  "Best Writing"
)
val awardPriority = udf { a: String =>
  val idx = orderedAwards.indexOf(a)
  if (idx >= 0) idx + 1 else Int.MaxValue
}
val bestAwardByPriority = dfWithExtras
  .select(col("tconst"), explode(col("award_arr")).alias("award"))
  .withColumn("pr", awardPriority(col("award")))
  .filter(col("pr") < Int.MaxValue)
  .withColumn("rn", row_number().over(Window.partitionBy("tconst").orderBy("pr")))
  .filter(col("rn") === 1)
  .select("tconst", "award")
val dfAwardDim = spark.table("premioMasImportante")
val awardsExploded = bestAwardByPriority
  .join(dfAwardDim, $"award" === dfAwardDim("premio"), "left")
  .select($"tconst", coalesce($"id_premioMasImportante", lit(1)).as("best_award_id"))

// ----------------------------------------------------------------------------
// 6) Unir todo y calcular métricas
// ----------------------------------------------------------------------------
val joined = dfWithExtras
  .join(dfRatings, Seq("tconst"), "left")
  .join(netflixSet, Seq("tconst"), "left")
  .join(primeSet,   Seq("tconst"), "left")
  .join(appleSet,   Seq("tconst"), "left")
  .join(dfGuion, $"writer"   === $"g_nconst", "left")
  .join(dfDir,   $"director" === $"d_nconst", "left")
  .join(dfAct.as("a1"), $"actor1" === $"a1.a_nconst", "left")
  .join(dfAct.as("a2"), $"actor2" === $"a2.a_nconst", "left")
  .join(dfAct.as("a3"), $"actor3" === $"a3.a_nconst", "left")
  .join(dfProd, $"country" === $"prod_pais", "left")
  .join(dfEmp,  $"productionCompany" === $"emp_name", "left")
  .join(dfLan,  $"mes" <=> $"lan_mes" && $"anyo" <=> $"lan_anyo", "left")
  .join(dfLang, $"raw_lang" === $"lang_str", "left")
  .join(dfGen.as("G1"), $"g1" === $"G1.gen_str", "left")
  .join(dfGen.as("G2"), $"g2" === $"G2.gen_str", "left")
  .join(dfGen.as("G3"), $"g3" === $"G3.gen_str", "left")
  .join(awardsExploded, Seq("tconst"), "left")

val dfMedidas = joined
  // convertir monedas a eur (Long) y luego safe cast a Int
  .withColumn("presupuesto_long", toEur(col("budget")))
  .withColumn("recaudacion_long", toEur(col("grossRevenue")))
  .withColumn("presupuesto",
    when(col("presupuesto_long").isNull || col("presupuesto_long") > lit(Int.MaxValue),
         lit(null).cast("int"))
    .otherwise(col("presupuesto_long").cast("int"))
  )
  .withColumn("recaudacion",
    when(col("recaudacion_long").isNull || col("recaudacion_long") > lit(Int.MaxValue),
         lit(null).cast("int"))
    .otherwise(col("recaudacion_long").cast("int"))
  )
  .withColumn("beneficio",
    expr("""CASE WHEN presupuesto IS NULL OR recaudacion IS NULL
                  THEN NULL ELSE recaudacion - presupuesto END""").cast("int")
  )
  // demás métricas
  .withColumn("puntuacionMedia", col("averageRating").cast(FloatType))
  .withColumn("votos",           col("numVotes").cast("int"))
  .withColumn("duracion",        col("runtimeMinutes").cast("int"))
  .withColumn("premios",         col("awards").cast("int"))
  .withColumn("nominaciones",    col("nominations").cast("int"))
  // surrogate key ordenado por IMDb code
  .withColumn("id_contenidoAudiovisual",
    row_number().over(Window.orderBy(col("tconst")))
  )
  // campos básicos
  .withColumn("tconst", col("tconst"))
  .withColumn("titulo", col("primaryTitle"))
  .withColumn("id_tipo",
    when(col("titleType").isin("movie","tvMovie"), 1)
    .when(col("titleType")==="tvSeries",           2)
    .otherwise(1)
  )
  // plataforma
  .withColumn("sumFlags",
    coalesce(col("isNetflix"), lit(0)) +
    coalesce(col("isPrime"),   lit(0)) +
    coalesce(col("isApple"),   lit(0))
  )
  .withColumn("id_plataforma",
    when(col("sumFlags") === 0,    1)
    .when(col("sumFlags") > 1,     2)
    .when(col("isNetflix") === 1,  3)
    .when(col("isPrime")   === 1,  4)
    .when(col("isApple")   === 1,  5)
  )
  // guionista/director
  .withColumn("id_guionista",
    when(col("id_guionista").isNull, 1).otherwise(col("id_guionista"))
  )
  .withColumn("id_director",
    when(col("id_director").isNull, 1).otherwise(col("id_director"))
  )
  // actores
  .withColumn("id_actorPrincipal",
    when(col("a1.id_actor").isNull, 1).otherwise(col("a1.id_actor"))
  )
  .withColumn("id_actorSecundario1",
    when(col("a2.id_actor").isNull, 1).otherwise(col("a2.id_actor"))
  )
  .withColumn("id_actorSecundario2",
    when(col("a3.id_actor").isNull, 1).otherwise(col("a3.id_actor"))
  )
  // producción
  .withColumn("id_produccion",
    when(col("dim_id_produccion").isNull || lower(col("prod_pais")) === "desconocido", 1)
    .otherwise(col("dim_id_produccion"))
  )
  // empresa productora
  .withColumn("id_empresaProductora",
    when(col("id_empresaProductora").isNull || lower(col("emp_name")) === "desconocido", 1)
    .otherwise(col("id_empresaProductora"))
  )
  // lanzamiento
  .withColumn("id_lanzamiento",
    when(col("id_lanzamiento").isNull, 1).otherwise(col("id_lanzamiento"))
  )
  // idioma
  .withColumn("id_idioma",
    when(col("raw_lang").isNull || col("raw_lang")==="" || col("raw_lang")==="Desconocido", 2)
    .when(size(split(col("raw_lang"),",")) > 1,                                   1)
    .otherwise(col("id_idioma"))
  )
  // géneros
  .withColumn("id_generoPrincipal",
    when(col("G1.id_genero").isNull || col("g1")==="", 1).otherwise(col("G1.id_genero"))
  )
  .withColumn("id_generoSecundario1",
    when(col("G2.id_genero").isNull || col("g2")==="", 1).otherwise(col("G2.id_genero"))
  )
  .withColumn("id_generoSecundario2",
    when(col("G3.id_genero").isNull || col("g3")==="", 1).otherwise(col("G3.id_genero"))
  )
  // premio más importante
  .withColumn("id_premioMasImportante",
    coalesce(col("best_award_id"), lit(1))
  )
  // seleccionar columnas finales
  .select(
    "id_contenidoAudiovisual",
    "tconst",
    "titulo",
    "puntuacionMedia",
    "votos",
    "duracion",
    "premios",
    "nominaciones",
    "presupuesto",
    "recaudacion",
    "beneficio",
    "id_guionista",
    "id_director",
    "id_actorPrincipal",
    "id_actorSecundario1",
    "id_actorSecundario2",
    "id_tipo",
    "id_produccion",
    "id_plataforma",
    "id_premioMasImportante",
    "id_idioma",
    "id_generoPrincipal",
    "id_generoSecundario1",
    "id_generoSecundario2",
    "id_lanzamiento",
    "id_empresaProductora"
  )

// ----------------------------------------------------------------------------
// 7) Mostrar y escribir en Hive
// ----------------------------------------------------------------------------
dfMedidas.show(10,false)
dfMedidas.write.mode("overwrite").insertInto("contenidoAudiovisual")
