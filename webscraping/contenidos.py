import asyncio 
import aiohttp
import random
import os
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import urllib.parse  # Para decodificar correctamente las URLs

# =============================
# FUNCIONES DE UTILIDAD
# =============================

def convert_to_ddmmyyyy(date_str):
    month_names = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    try:
        month_name, day_year = date_str.split(" ", 1)
        day, year = day_year.split(",", 1)
        month = month_names.get(month_name.strip(), None)
        day = day.strip()
        year = year.strip()
        if month:
            return f"{day}/{month}/{year}"
        else:
            return None
    except Exception as e:
        print(f"Error al convertir la fecha: {e}")
        return None

# =============================
# CONFIGURACIÓN Y VARIABLES GLOBALES
# =============================

base_url = "https://www.imdb.com/title/tt"
input_file = "title.basics.tsv"
output_file = "title.basics_webscrapping2.tsv"

# Lista completa de User-Agent (original)
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; SM-A528B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) CriOS/120.0.0.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_5 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) CriOS/118.0.0.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_3; rv:119.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Android 13; Mobile; rv:120.0) Gecko/120.0 Firefox/120.0",
    "Mozilla/5.0 (Android 12; Mobile; rv:119.0) Gecko/119.0 Firefox/119.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) FxiOS/120.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 15_2 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) FxiOS/118.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/537.36 (KHTML, like Gecko) Version/16.2 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/537.36 (KHTML, like Gecko) Version/15.5 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/537.36"
]

# Puedes ajustar el semáforo; aquí lo dejamos en 5 para evitar bloqueos
semaphore = asyncio.Semaphore(5)

# =============================
# FUNCIONES ASÍNCRONAS PARA WEBSCRAPING
# =============================

async def fetch(session, url, retries=3):
    headers = {"User-Agent": random.choice(user_agents)}
    delay = 0.5
    for attempt in range(retries):
        async with semaphore:
            try:
                async with session.get(url, headers=headers) as response:
                    # Si obtenemos un 429 (demasiadas solicitudes), esperamos con retroceso exponencial
                    if response.status == 429:
                        wait_time = delay * (2 ** attempt)
                        print(f"429 detectado en {url}. Esperando {wait_time} segundos...")
                        await asyncio.sleep(wait_time)
                        continue
                    # Delay para simular navegación humana (0.5-1 s)
                    await asyncio.sleep(random.uniform(0.5, 1))
                    text = await response.text()
                    return text, response.status
            except Exception as e:
                print(f"Error en {url} (intento {attempt+1}/{retries}): {e}")
                await asyncio.sleep(delay * (2 ** attempt))
    return None, None

async def get_oscars_won(session, numeric_id):
    url_awards = f"{base_url}{numeric_id}/awards"
    html, status = await fetch(session, url_awards)
    if html is None or status == 404:
        return []
    soup = BeautifulSoup(html, "lxml")
    print(f"Solicitando la página de premios para tt{numeric_id}...")
    span_tag = soup.select_one("span#ev0000003")
    oscars_won = []
    if span_tag and "Academy Awards, USA" in span_tag.text:
        awards_section = soup.find("div", {"data-testid": "sub-section-ev0000003"})
        if awards_section:
            for li in awards_section.find_all("li", {"data-testid": "list-item"}):
                award_link = li.select_one("a.ipc-metadata-list-summary-item__t")
                if award_link and ("Ganador" in award_link.text or "Winner" in award_link.text):
                    winner_tag = li.select_one("span.ipc-metadata-list-summary-item__tst")
                    if winner_tag and "Oscar" in winner_tag.text:
                        award_category = li.select_one("span.ipc-metadata-list-summary-item__li.awardCategoryName")
                        if award_category:
                            oscars_won.append(award_category.text.strip())
        if oscars_won:
            print(f"Oscars: {oscars_won}")
        else:
            print("No se encontraron Oscars.")
        return oscars_won
    print(f"Sección de premios no encontrada para tt{numeric_id}.")
    return []

async def process_movie(session, tconst):
    numeric_id = tconst.replace("tt", "")
    url = f"{base_url}{numeric_id}/"
    html, status = await fetch(session, url)
    if html is None or status == 404:
        print(f"Error 404: Página no encontrada para tt{tconst}. Saltando...")
        return None
    try:
        soup = BeautifulSoup(html, "lxml")
        # NOTA: Se eliminan la extracción de 'rating', 'votes', 'directors' y 'writers'
        
        # Luego en la fecha el día no me interesa, me interesa el mes y el año
        # Awards y nominations
        awards_count = 0
        nominations_count = 0
        awards_section = soup.find("div", {"data-testid": "awards"})
        if awards_section:
            awards_info = awards_section.find("li", {"data-testid": "award_information"})
            if awards_info:
                awards_text = awards_info.get_text(strip=True)
                awards_match = re.search(r"(\d+)\s*(premios?|wins?)", awards_text)
                nominations_match = re.search(r"(\d+)\s*(nominaciones?|nominations?)", awards_text)
                awards_count = int(awards_match.group(1)) if awards_match else 0
                nominations_count = int(nominations_match.group(1)) if nominations_match else 0

        budget_tag = soup.select_one("li[data-testid='title-boxoffice-budget'] span.ipc-metadata-list-item__list-content-item")
        budget = budget_tag.text.strip() if budget_tag else "Desconocido"

        gross_tag = soup.select_one("li[data-testid='title-boxoffice-cumulativeworldwidegross'] span.ipc-metadata-list-item__list-content-item")
        gross = gross_tag.text.strip() if gross_tag else "Desconocido"

        company_tag = soup.select_one("li[data-testid='title-details-companies'] a.ipc-metadata-list-item__list-content-item--link")
        production_company = company_tag.text.strip() if company_tag else "Desconocido"

        release_date_tag = soup.select_one("li[data-testid='title-details-releasedate'] a.ipc-metadata-list-item__list-content-item--link")
        if release_date_tag:
            release_date = release_date_tag.text.strip()
            release_date = re.sub(r"\s*\(.*\)", "", release_date)
            release_date = convert_to_ddmmyyyy(release_date)
        else:
            release_date = "Desconocido"

        language_tags = soup.select("li[data-testid='title-details-languages'] a.ipc-metadata-list-item__list-content-item--link")
        languages_list = [tag.text.strip() for tag in language_tags] if language_tags else ["Desconocido"]

        country_tag = soup.select_one("li[data-testid='title-details-origin'] a.ipc-metadata-list-item__list-content-item--link")
        country = country_tag.text.strip() if country_tag else "Desconocido"

        # Extracción del reparto (cast)
        cast_codes = []
        credit_sections = soup.find_all("li", {"data-testid": "title-pc-principal-credit"})
        for section in credit_sections:
            label = section.find(lambda tag: tag.name in ["span", "a"] and "ipc-metadata-list-item__label" in tag.get("class", []))
            if label and any(kw in label.get_text(strip=True) for kw in ["Reparto", "Stars", "Cast", "Full Cast", "Star"]):
                ul = section.find("ul", {"role": "presentation"})
                if ul:
                    for actor in ul.find_all("a", class_="ipc-metadata-list-item__list-content-item--link"):
                        href = actor.get("href")
                        if href:
                            m = re.search(r'/name/(nm\d+)/', href)
                            if m:
                                cast_codes.append(m.group(1))
        cast_codes = list(dict.fromkeys(cast_codes))[:3]

        # Oscars ganados
        oscars_won = await get_oscars_won(session, numeric_id)

    except Exception as e:
        print(f"Error procesando tt{tconst}: {e}. Saltando este título.")
        return None

    await asyncio.sleep(random.uniform(1, 2))
    print(f"Procesado {tconst} - Reparto: {', '.join(cast_codes)}")
    
    return {
        "tconst": tconst,
        "awards": awards_count,
        "nominations": nominations_count,
        "budget": budget,
        "grossRevenue": gross,
        "productionCompany": production_company,
        "releaseDate": release_date,
        "languages": ", ".join(languages_list),
        "country": country,
        "cast": ", ".join(cast_codes) if cast_codes else "",
        "oscarsWon": oscars_won
    }

# =============================
# FUNCIÓN PRINCIPAL ASÍNCRONA
# =============================

async def main():
    try:
        df_titles = pd.read_csv(input_file, sep="\t", na_values="\\N", dtype=str)
    except Exception as e:
        print(f"Error al leer {input_file}: {e}")
        return

    if os.path.exists(output_file):
        try:
            df_output = pd.read_csv(output_file, sep="\t", dtype=str)
            # Extraemos la lista de tconst que ya se han procesado
            processed_ids = df_output["tconst"].tolist()
            # Filtramos el DataFrame de títulos, dejando solo aquellos que no estén en processed_ids
            df_titles = df_titles[~df_titles["tconst"].isin(processed_ids)]
            print(f"Continuando scraping. Se procesarán {len(df_titles)} títulos nuevos.")
        except Exception as e:
            print(f"Error al leer {output_file}: {e}")

    tconst_list = df_titles["tconst"].tolist()
    total_titles = len(tconst_list)
    chunk_size = 1000

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        for idx in range(0, total_titles, chunk_size):
            current_chunk = tconst_list[idx: idx + chunk_size]
            tasks = [asyncio.create_task(process_movie(session, tconst)) for tconst in current_chunk]
            if len(tasks) >= 50:
                print("⏸️ Pausa corta dentro del chunk...")
                await asyncio.sleep(random.randint(5, 10))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            # Filtramos solo las respuestas válidas (con diccionario)
            chunk_results = [res for res in responses if isinstance(res, dict)]
            
            if chunk_results:
                df_web = pd.DataFrame(chunk_results)
                # Solo seleccionamos los títulos que se procesaron correctamente
                df_chunk_original = df_titles[df_titles['tconst'].isin(df_web['tconst'])]
                df_merged_chunk = pd.merge(df_chunk_original, df_web, on="tconst", how="left")
                if idx == 0 and not os.path.exists(output_file):
                    df_merged_chunk.to_csv(output_file, sep="\t", index=False)
                else:
                    df_merged_chunk.to_csv(output_file, sep="\t", index=False, mode='a', header=False)
                print(f"Guardado chunk: procesados {len(df_web)} de {len(current_chunk)} registros exitosos de {total_titles} totales.")
            else:
                print("No se procesaron películas en este chunk (posible error 404). Se reintentará en próximas ejecuciones.")

            print("⏸️ Pausa corta entre chunks...")
            await asyncio.sleep(random.randint(10, 20))

if __name__ == '__main__':
    asyncio.run(main())
