# Web Scraping

Esta carpeta contiene scripts en **Python** destinados a extraer y enriquecer datos mediante técnicas de **web scraping**, tanto de **contenidos audiovisuales** como de **profesionales de la industria**.

## Fuentes de datos

- **IMDb** (Internet Movie Database): Base de datos de referencia con información de películas, series y profesionales. No todos los atributos (presupuesto, recaudación, premios, detalles biográficos) están disponibles en los archivos oficiales, por lo que realizamos scraping de páginas de título (`ttXXXXXXX`) y persona (`nmXXXXXXX`).

- **JustWatch**: Plataforma-guía de streaming que muestra la disponibilidad de títulos en servicios como Netflix, Prime Video y Apple TV+. No ofrece API pública, por lo que usamos scraping dinámico con **Selenium** para simular scroll y cargar todos los ítems.

---

## Tecnologías y librerías

- **Python 3.x**
- **aiohttp**, **asyncio**: peticiones HTTP concurrentes.
- **BeautifulSoup**: parseo de HTML.
- **Selenium WebDriver**: navegación y scroll dinámico.
- **Pandas**: manipulación y almacenamiento en CSV/TSV.
- **Re**, **urllib.parse**, **datetime**: limpieza y parsing de cadenas.
- **Threading** y **pausas aleatorias**: para evitar bloqueos por IP.

---

## Scripts disponibles

| Archivo             | Descripción                                                                                                                                                                           |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `actprodwriters.py` | Scrapea páginas de IMDb para extraer datos de personas (actores, directores, guionistas): fecha y lugar de nacimiento, premios y si ganaron Oscar. Utiliza **aiohttp** y **asyncio**. |
| `contenidos.py`     | Scrapea las páginas de título en IMDb para extraer presupuesto, recaudación, fecha de lanzamiento, idiomas, país, reparto (hasta 3 miembros) y Oscars ganados.                        |
| `netflix.py`        | Extrae títulos disponibles en Netflix desde JustWatch en español. Simula scroll y obtiene hasta 300 títulos únicos.                                                                   |
| `primevideo.py`     | Igual que `netflix.py`, pero para Prime Video.                                                                                                                                        |
| `appletvplus.py`    | Igual que `netflix.py`, pero para Apple TV+.                                                                                                                                          |

---

## Código

1. **Inicialización**: configuración de cabeceras HTTP (`User-Agent`), semáforo para concurrencia y para evitar bloqueos.
2. **Extracción**:
   - Con **aiohttp**/**asyncio** para IMDb (peticiones concurrentes, retries, delays).
   - Con **Selenium** para JustWatch (apertura de navegador, scroll dinámico, reintentos).
3. **Parseo** con BeautifulSoup: navegación del DOM y extracción de campos clave.
4. **Limpieza** y **normalización**: eliminación de caracteres especiales, conversión de formatos de fecha, regex.
5. **Persistencia**: almacenamiento incremental en archivos TSV/CSV (`*.tsv`, `*.csv`) para su posterior procesamiento por los ETLs.

---

> *Nota: Cada script incluye parámetros ajustables (chunk sizes, timeouts, user-agents) y manejo de reanálisis en caso de fallos. Consulta código de cada fichero para más detalles.*

