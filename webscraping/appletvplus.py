from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import random
import time
from datetime import datetime, timedelta

# Configuración de Selenium con Chrome
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# URLs de JustWatch para Netflix en español
url_es = 'https://www.justwatch.com/es/proveedor/apple-tv-plus'

# Lista de User-Agent para simular diferentes navegadores
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36 Edge/91.0.864.59",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
]

# Función para obtener el HTML de una URL con un User-Agent aleatorio
def get_html(url):
    user_agent = random.choice(user_agents)
    driver.get(url)
    time.sleep(random.uniform(1, 5))
    return driver.page_source

# Función para hacer scroll y cargar más películas
def load_more_movies():
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(2, 4))

# Leer el archivo CSV si existe, de lo contrario, crear una lista vacía
try:
    df_existing = pd.read_csv('appletvplus.csv')
    existing_titles = df_existing['Titulo en Espanol'].tolist()
    last_id = df_existing['id'].max()
except FileNotFoundError:
    existing_titles = []
    last_id = 0

# Lista para nuevos títulos
movie_titles_es = []
max_movies = 300
current_movies = 0
last_movie_added_time = datetime.now()
timeout_duration = timedelta(minutes=2)

# Extraer títulos de la página
print("\nExtrayendo títulos:")
html_es = get_html(url_es)
soup_es = BeautifulSoup(html_es, 'html.parser')

while current_movies < max_movies:
    movies_es = soup_es.find_all('div', attrs={'data-title': True})
    new_movies_added = False
    
    for index, movie in enumerate(movies_es[current_movies:], start=current_movies + 1):
        title_es = movie.get('data-title')
        
        # Comparación exacta para evitar perder películas por coincidencias parciales
        if not any(title_es == existing_title for existing_title in existing_titles):
            movie_titles_es.append(title_es)
            existing_titles.append(title_es)
            print(f"Película {index}: {title_es}")
            current_movies += 1
            new_movies_added = True
            last_movie_added_time = datetime.now()
            if current_movies >= max_movies:
                break
    
    if not new_movies_added and datetime.now() - last_movie_added_time > timeout_duration:
        print("\nNo se han añadido nuevas películas en los últimos 2 minutos. Terminando ejecución.")
        break
    
    load_more_movies()
    html_es = driver.page_source
    soup_es = BeautifulSoup(html_es, 'html.parser')

# Generar IDs únicos
new_ids = range(last_id + 1, last_id + len(movie_titles_es) + 1)

# Crear DataFrame con nuevos títulos
df_new = pd.DataFrame({'id': new_ids, 'Titulo en Espanol': movie_titles_es})

# Combinar con títulos existentes
df_combined = pd.DataFrame({'id': range(1, len(existing_titles) + 1), 'Titulo en Espanol': existing_titles})
df_combined = pd.concat([df_combined, df_new]).drop_duplicates(subset=['Titulo en Espanol']).reset_index(drop=True)

df_combined.to_csv('appletvplus.csv', index=False)

print("\nExtracción completa. DataFrame generado:")
print(df_combined)

driver.quit()
