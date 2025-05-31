import sys
import asyncio
if sys.platform.startswith("win"):
    # Cambiamos la política del event loop para evitar problemas con el Proactor
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import random
import os
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib.parse  # Para decodificar correctamente las URLs
import re  # Para expresiones regulares

# Base URL de IMDb para nombres (en español)
base_url = "https://www.imdb.com/es-es/name/"

# Ficheros de entrada y salida
base_file = "name.basics.tsv"  # Fichero base con campos: nconst, primaryName, etc.
output_file = "name.basics_webscrapping.tsv"         # Fichero resultante en formato TSV

# Lista completa de User-Agent
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Mozilla/5.0 (Windows NT 6.3; rv:41.0) Gecko/20100101 Firefox/41.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36 Edge/85.0.564.63",
    "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; AS; AS; T7348) like Gecko",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; rv:40.0) Gecko/20100101 Firefox/40.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36 Edge/94.0.992.38",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edge/87.0.664.66",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0",
    "Mozilla/5.0 (Windows NT 6.3; Trident/7.0; AS; AS; T7348) like Gecko",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.93 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 13_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.71 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; AS; T7348) like Gecko",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 9; SAMSUNG SM-J730G) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/14.2 Chrome/92.0.4515.166 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 8.1.0; Redmi Note 5 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.216 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.87 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62",
    "Mozilla/5.0 (Android 11; Mobile; LG-M255) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0",
    "Mozilla/5.0 (Linux; Android 9; SAMSUNG SM-A530F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.101 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 15_4 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.62 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.178 Safari/537.36 Edg/110.0.1587.78",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:93.0) Gecko/20100101 Firefox/93.0",
    "Mozilla/5.0 (Linux; Android 7.1.2; SM-T820) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_8_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 8.0.0; SM-G950U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_16_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15",
    "Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.5195.136 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 12; SM-T510) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.92 Safari/537.36",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Linux; Android 11; SM-N986B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.97 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.5359.124 Safari/537.36",
    "Mozilla/5.0 (iPod touch; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; Mi A3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36"
]

# Semáforo para limitar el número de conexiones concurrentes
semaphore = asyncio.Semaphore(9)

async def fetch(session, url, retries=3):
    """Intenta obtener el contenido de la URL hasta 'retries' veces."""
    for attempt in range(retries):
        try:
            headers = {"User-Agent": random.choice(user_agents)}
            async with semaphore:
                async with session.get(url, headers=headers) as response:
                    # Pausa corta aleatoria para evitar sobrecargas
                    await asyncio.sleep(random.uniform(0.5, 3))
                    return await response.text()
        except Exception as e:
            print(f"Error al obtener la URL {url} en intento {attempt + 1}: {e}")
            await asyncio.sleep(1)  # Pequeña pausa antes de reintentar
    return None

# Función para obtener la cantidad de premios ganados
async def get_award_count(session, person_id):
    awards_url = f"https://www.imdb.com/name/nm{person_id}/awards"
    html = await fetch(session, awards_url)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        award_text_tag = soup.find("div", {"class": "ipc-signpost__text", "role": "presentation"})
        if award_text_tag:
            award_text = award_text_tag.text.strip()
            match = re.match(r"(\d+)\s+win(?:s)?", award_text, re.IGNORECASE)
            if match:
                return int(match.group(1))
    return 0

# Función para verificar si ha ganado un Oscar
async def has_won_oscar(session, person_id):
    url_person = f"https://www.imdb.com/name/nm{person_id}"
    html = await fetch(session, url_person)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        oscar_tag = soup.find("a", string=re.compile(r"won \d+ oscar", re.IGNORECASE))
        if oscar_tag:
            return True
    return False

# Función para extraer datos adicionales de una persona mediante scraping
async def extract_person_data(session, person_row):
    nconst = person_row["nconst"]  # Ejemplo: "nm0000001"
    person_id = nconst[2:]         # Ejemplo: "0000001"
    print(f"Procesando {nconst}")
    url = f"{base_url}{nconst}/"
    
    try:
        html = await fetch(session, url)
        if not html:
            print(f"No se pudo obtener contenido para {nconst}")
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Extraer fecha de nacimiento detallada
        birthdate = None
        birthdate_tag = soup.find("div", {"data-testid": "birth-and-death-birthdate"})
        if birthdate_tag:
            spans = birthdate_tag.find_all("span")
            if spans:
                birthdate_text = spans[-1].text.strip()
                month_translation = {
                    "enero": "January", "febrero": "February", "marzo": "March", "abril": "April",
                    "mayo": "May", "junio": "June", "julio": "July", "agosto": "August",
                    "septiembre": "September", "octubre": "October", "noviembre": "November", "diciembre": "December"
                }
                for es_month, en_month in month_translation.items():
                    birthdate_text = birthdate_text.replace(f" de {es_month} de ", f" {en_month} ")
                try:
                    birthdate = datetime.strptime(birthdate_text, "%d %B %Y").strftime("%d/%m/%Y")
                except ValueError:
                    print(f"Fecha de nacimiento no válida: {birthdate_text}")

        # Extraer lugar de nacimiento con la lógica modificada:
        # Ejemplo: "Vigo, Pontevedra, Galicia, España" → se ignora "Vigo"
        birth_city, birth_state, birth_country = None, None, None
        personal_details_section = soup.find("section", {"data-testid": "PersonalDetails"})
        if personal_details_section:
            birth_place_tag = personal_details_section.find("li", {"data-testid": "nm_pd_bl"})
            if birth_place_tag:
                birth_place_link = birth_place_tag.find("a", {"class": "ipc-metadata-list-item__list-content-item--link"})
                if birth_place_link:
                    birth_place_text = birth_place_link.text.strip()
                    decoded_place_text = urllib.parse.unquote(birth_place_text)
                    decoded_place_text = re.sub(r"\[.*?\]", "", decoded_place_text).strip()
                    parts = list(dict.fromkeys(decoded_place_text.split(", ")))
                    if len(parts) == 4:
                        # Se ignora el primer elemento
                        birth_city = parts[1]
                        birth_state = parts[2]
                        birth_country = parts[3]
                    elif len(parts) == 3:
                        birth_city, birth_state, birth_country = parts
                    elif len(parts) == 2:
                        birth_city = None
                        birth_state, birth_country = parts
                    elif len(parts) == 1:
                        birth_city = None
                        birth_state = None
                        birth_country = parts[0]

        awards_count = await get_award_count(session, person_id)
        won_oscar = await has_won_oscar(session, person_id)

        # Combinar la información base (del fichero) con la extraída por scraping
        result = {
            "nconst": nconst,
            "primaryName": person_row.get("primaryName"),
            "birthYear": person_row.get("birthYear"),
            "deathYear": person_row.get("deathYear"),
            "primaryProfession": person_row.get("primaryProfession"),
            "knownForTitles": person_row.get("knownForTitles"),
            "Birth Date": birthdate,
            "City of Birth": birth_city,
            "State of Birth": birth_state,
            "Country of Birth": birth_country,
            "Awards Count": awards_count,
            "Won Oscar": won_oscar
        }
        
        print(f"Procesado {nconst}: {result['primaryName']}, {birthdate}, {birth_city}, {birth_state}, {birth_country}, Premios: {awards_count}, Oscar: {won_oscar}")
        return result

    except Exception as e:
        print(f"Error al procesar {nconst}: {e}")
        return None

def get_chunks(data_list, chunk_size):
    """Generador para dividir la lista de datos en chunks de tamaño chunk_size."""
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i+chunk_size]

async def process_and_save_data():
    # Cargar el fichero base filtrado (TSV)
    try:
        df_base = pd.read_csv(base_file, sep='\t')
    except Exception as e:
        print(f"Error al leer {base_file}: {e}")
        return

    # Si ya existe un fichero de salida, obtener el último nconst procesado
    last_id = None
    if os.path.exists(output_file):
        try:
            df_out = pd.read_csv(output_file, sep='\t')
            if not df_out.empty:
                # Extraemos el máximo valor numérico del campo "nconst"
                last_id = df_out["nconst"].apply(lambda x: int(x[2:])).max()
                print(f"Reanudando procesamiento desde nconst > nm{str(last_id).zfill(7)}")
            else:
                print("El fichero de salida está vacío. Procesando todo el fichero base.")
        except Exception as e:
            print(f"Error al leer {output_file}: {e}")

    # Filtrar los registros del fichero base: si last_id se definió, conservar solo aquellos con nconst numérico mayor
    if last_id is not None:
        df_base["num"] = df_base["nconst"].apply(lambda x: int(x[2:]))
        df_filtered = df_base[df_base["num"] > last_id].drop(columns="num")
    else:
        df_filtered = df_base

    people_data = df_filtered.to_dict(orient="records")
    total_to_process = len(people_data)
    print(f"Quedan por procesar {total_to_process} registros.")

    chunk_size = 1000  # Procesar 1000 registros por chunk
    chunk_index = 0

    # Procesar y guardar los registros en chunks
    async with aiohttp.ClientSession() as session:
        for chunk in get_chunks(people_data, chunk_size):
            tasks = []
            for person_row in chunk:
                tasks.append(asyncio.create_task(extract_person_data(session, person_row)))
            results_chunk = await asyncio.gather(*tasks)
            valid_results = [res for res in results_chunk if res is not None]
            # Ordenar el chunk (opcional; los datos globales estarán en orden si el fichero base lo estaba)
            valid_results.sort(key=lambda x: int(x["nconst"][2:]))
            df_chunk = pd.DataFrame(valid_results)
            if chunk_index == 0 and last_id is None:
                # Si es el primer chunk y no se está reanudando, sobrescribir
                df_chunk.to_csv(output_file, sep='\t', index=False, mode='w')
            else:
                # En modo append
                df_chunk.to_csv(output_file, sep='\t', index=False, mode='a', header=False)
            print(f"Guardado chunk {chunk_index+1} con {len(valid_results)} registros en {output_file}")
            chunk_index += 1

    print(f"Datos guardados completamente en {output_file}")

if __name__ == '__main__':
    asyncio.run(process_and_save_data())
