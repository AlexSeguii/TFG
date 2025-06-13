# Procesos ETL

En esta carpeta se encuentran los scripts de los procesos ****ETL**** (Extracción, Transformación y Carga) implementados en ****Apache Spark**** con ****Scala****, ejecutados sobre ****Apache Zeppelin****.

## Función de los ETLs

Cada script ETL se encarga de:

1. **Extracción**: lectura de datos en bruto desde archivos TSV/CSV y APIs/webscraping.
2. **Transformación**: limpieza, normalización y enriquecimiento de datos.
3. **Carga:** escritura de los datos procesados en las tablas de **Hive**.

---

## Tecnologías y entornos

- **Apache Spark** (Scala)
- **Apache Zeppelin** notebooks
- Librerías de webscraping en **Python** (BeautifulSoup, Selenium) para enriquecer datos de IMDb y JustWatch.
- **Hive**: destino de carga, sin claves foráneas y con tipos ajustados (`STRING`, `BOOLEAN`, etc.).

---

## Listado de ETL Scripts

| Scrip breve                         | Descripción                                           |
| ----------------------------------- | ----------------------------------------------------- |
| `etl_actor.scala`                   | Carga y limpia datos de actores (name.basics.tsv).    |
| `etl_director.scala`                | Carga y limpia datos de directores (name.basics.tsv). |
| `etl_guionista.scala`               | Carga y limpia datos de guionistas (name.basics.tsv). |
| `etl_empresaProductora.scala`       | Procesa `productionCompany` de title.basics.tsv.      |
| `etl_genero.scala`                  | Explode y normaliza géneros de title.basics.tsv.      |
| `etl_idiomas.scala`                 | Limpia y traduce idiomas de title.basics.tsv.         |
| `etl_lanzamiento.scala`             | Genera jerarquías de fecha (mes, época, año).         |
| `etl_tipo.scala`                    | Filtra y normaliza tipos.                             |
| `etl_plataforma.scala`              | Define valores de plataforma (Netflix, Prime, Apple). |
| `etl_premiomasimportante.scala`     | Limpia y selecciona premios Oscar (oscarsWon).        |
| `etl_produccion.scala`              | Limpia y normaliza países de producción.              |
| `etl_h_contenido_audiovisual.scala` | Construye la tabla de hechos con joins y medidas.     |

---

## Flujo general de un ETL

1. **Lectura** del origen (TSV, CSV).
2. **Preprocesado**: parseo de formatos, eliminación de nulos y duplicados.
3. **Limpieza**: normalización de texto (minúsculas, acentos), traducción, regex.
4. **Enriquecimiento**: webscraping para presupuesto, recaudación, metadatos personales.
5. **Uniones (Joins)** con otros datasets (e.g., ratings, contenidos de streaming).
6. **Cálculos**: medidas derivadas (beneficio = recaudación – presupuesto).
7. **Escritura** en Hive.

---

> **Nota:** Se puede encontrar el código de cada ETL correspondiente en esta carpeta.

