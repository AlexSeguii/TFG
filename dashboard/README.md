# Datasets

Dado a las limitaciones de GitHub y el gran tamaño de los ficheros estos podrán ser accedidos a partir del siguiente enlace en Drive.

[Acceder al Drive de datasets](https://drive.google.com/drive/folders/196GpKG1rH3CJ52-_V8KF4zKeLyZ5U2sW?dmr=1&ec=wgc-drive-hero-goto)

## Fuentes de datos

### 1. IMDb – Non Commercial Datasets

La página de IMDb Non Commercial Datasets ofrece descargas diarias de archivos TSV con información detallada de películas, series y profesionales de la industria audiovisual.

#### Archivos principales

- **title.basics.tsv**: Información general de contenidos.
  - Filas: 1 171 070 (1974–2032).
  - Actualización: diaria.

| Campo          | Tipo    | Descripción                                     | Ejemplo         |
| -------------- | ------- | ----------------------------------------------- | --------------- |
| tconst         | String  | ID de contenido en IMDb                         | tt0332452       |
| titleType      | String  | Tipo (Movie, TVSeries, etc.)                    | Movie           |
| primaryTitle   | String  | Título más popular                              | Troy            |
| originalTitle  | String  | Título original                                 | Troy            |
| isAdult        | Boolean | Contenido para adultos (0=NO, 1=SÍ)             | 0               |
| startYear      | Int     | Año de estreno                                  | 2004            |
| endYear        | Int     | Año de finalización (NULL si sigue emitiéndose) | NULL            |
| runtimeMinutes | Int     | Duración en minutos                             | 163             |
| genres         | String  | Géneros                                         | Adventure,Drama |
| languages      | String  | Idiomas                                         | English         |

- **title.ratings.tsv**: Puntuaciones y número de votos.
  - Filas: 1 454 241.
  - Actualización: diaria.

| Campo         | Tipo   | Descripción             | Ejemplo   |
| ------------- | ------ | ----------------------- | --------- |
| tconst        | String | ID de contenido en IMDb | tt0332452 |
| averageRating | Float  | Puntuación media        | 7.3       |
| numVotes      | Int    | Número de votos         | 574 440   |

- **name.basics.tsv**: Datos de profesionales (actores, directores, etc.).
  - Filas: 1 301 485.
  - Actualización: diaria.

| Campo             | Tipo   | Descripción                              | Ejemplo                                 |
| ----------------- | ------ | ---------------------------------------- | --------------------------------------- |
| nconst            | String | ID de la persona                         | nm0000093                               |
| primaryName       | String | Nombre de la persona                     | Brad Pitt                               |
| birthYear         | Int    | Año de nacimiento                        | 1963                                    |
| deathYear         | Int    | Año de fallecimiento (\N si vivo)        | \N                                      |
| primaryProfession | String | Profesiones                              | producer,actor,executive                |
| knownForTitles    | String | Títulos destacados                       | tt0137523,tt0356910,tt1210166,tt0114746 |

### 2. JustWatch (Web scraping)

JustWatch es una guía gratuita de streaming. Se extrajo la disponibilidad diaria de contenidos en Netflix, Prime Video y Apple TV mediante scripts de web scraping.

| Archivo     | Filas | Plataformas | Campos                                           | Descripción                                  |
| ----------- | ----- | ----------- | ------------------------------------------------ | -------------------------------------------- |
| netflix.csv | 1 924 | Netflix     | title\_spanish, tconst, actors, director, writer | Titulos disponibles en Netflix (en español). |
| prime.csv   | 1 814 | Prime Video | title\_spanish, tconst, actors, director, writer | Titulos disponibles en Prime Video.          |
| appletv.csv | 245   | Apple TV    | title\_spanish, tconst, actors, director, writer | Titulos disponibles en Apple TV.             |

> **Nota:** Todos los archivos de JustWatch cubren contenidos de 1927 a 2025 y fueron descargados en mayo.

