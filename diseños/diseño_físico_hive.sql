%hive

-- Eliminar la base de datos anterior (para empezar desde cero)
DROP DATABASE IF EXISTS mydb CASCADE;

-- Crear la base de datos
CREATE DATABASE mydb;

-- Usar la base de datos creada
USE mydb;

-- -----------------------------------------------------
-- Tabla: guionista
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS guionista (
  id_guionista INT,
  nconst STRING,          -- IMDB ID del guionista
  nombre STRING,
  grupo_edad STRING,
  reputacion STRING,
  ciudad STRING,
  estado STRING,
  pais STRING,
  ganadorOscar BOOLEAN
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: director
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS director (
  id_director INT,
  nconst STRING,          -- IMDB ID del director
  nombre STRING,
  grupo_edad STRING,
  reputacion STRING,
  ciudad STRING,
  estado STRING,
  pais STRING,
  ganadorOscar BOOLEAN
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: actor
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS actor (
  id_actor INT,
  nconst STRING,          -- IMDB ID del actor
  nombre STRING,
  grupo_edad STRING,
  reputacion STRING,
  ciudad STRING,
  estado STRING,
  pais STRING,
  ganadorOscar BOOLEAN
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: tipo
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS tipo (
  id_tipo INT,
  tipo STRING
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: produccion
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS produccion (
  id_produccion INT,
  pais STRING
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: plataforma
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS plataforma (
  id_plataforma INT,
  plataforma STRING
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: premioMasImportante
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS premioMasImportante (
  id_premioMasImportante INT,
  premio STRING
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: idioma
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS idioma (
  id_idioma INT,
  idioma STRING
  -- multilenguaje BOOLEAN
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: genero
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS genero (
  id_genero INT,
  genero STRING
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: lanzamiento
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS lanzamiento (
  id_lanzamiento INT,
  mes INT,
  epoca STRING,
  anyo INT
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: empresaProductora
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS empresaProductora (
  id_empresaProductora INT,
  empresaProductora STRING
)
STORED AS PARQUET;

-- -----------------------------------------------------
-- Tabla: contenidoAudiovisual
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS contenidoAudiovisual (
  id_contenidoAudiovisual INT,
  tconst String,
  titulo String,
  puntuacionMedia FLOAT,
  votos INT,
  duracion INT,
  premios INT,
  nominaciones INT,
  presupuesto INT,
  recaudacion INT,
  beneficio INT,
  id_guionista INT,
  id_director INT,
  id_actorPrincipal INT,
  id_actorSecundario1 INT,
  id_actorSecundario2 INT,
  id_tipo INT,
  id_produccion INT,
  id_plataforma INT,
  id_premioMasImportante INT,
  id_idioma INT,
  id_generoPrincipal INT,
  id_generoSecundario1 INT,
  id_generoSecundario2 INT,
  id_lanzamiento INT,
  id_empresaProductora INT
)
STORED AS PARQUET;
