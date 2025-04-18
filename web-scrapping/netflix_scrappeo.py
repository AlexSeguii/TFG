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
url_es = 'https://www.justwatch.com/es/proveedor/netflix'

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
    user_agent = random.choice(user_agents)  # Selecciona un User-Agent aleatorio
    headers = {'User-Agent': user_agent}
    driver.get(url)
    time.sleep(random.uniform(1, 5))  # Pausa aleatoria entre cada iteración
    return driver.page_source

# Función para hacer scroll y cargar más películas
def load_more_movies():
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(2, 4))  # Espera entre desplazamientos

# Leer el archivo CSV si existe, de lo contrario, crear una lista vacía
try:
    df_existing = pd.read_csv('peliculas_netflix.csv')
    existing_titles = df_existing['Titulo en Espanol'].tolist()
    last_id = df_existing['id'].max()  # Obtener el último id utilizado
except FileNotFoundError:
    existing_titles = []
    last_id = 0  # Si el archivo no existe, comenzamos desde el id 0

# Crear lista para los títulos en español
movie_titles_es = []

# Inicializar el número de películas que se quieren extraer
max_movies = 1000  # Incrementamos el límite de películas a 1000 o más
current_movies = 0

# Tiempo de espera entre recargas (por ejemplo, 2 minutos sin añadir ninguna película)
last_movie_added_time = datetime.now()  # Hora de la última adición de película
timeout_duration = timedelta(minutes=2)  # Duración del tiempo máximo sin añadir películas

# Procesar las películas en español
print("\nExtrayendo títulos:")

# Leer el HTML inicial
html_es = get_html(url_es)
soup_es = BeautifulSoup(html_es, 'html.parser')

# Cargar películas hasta el límite
while current_movies < max_movies:
    # Encontrar los contenedores que contienen los títulos de las películas
    movies_es = soup_es.find_all('div', attrs={'data-title': True})
    
    # Extraer y mostrar títulos
    new_movies_added = False  # Bandera para saber si se han añadido nuevas películas
    for index, movie in enumerate(movies_es[current_movies:], start=current_movies + 1):
        title_es = movie.get('data-title')  # Extraemos el título en español
        
        # Si el título no está en la lista existente, lo agregamos
        if title_es not in existing_titles:
            movie_titles_es.append(title_es)
            existing_titles.append(title_es)  # Añadir a la lista de títulos existentes
            print(f"Película {index}: {title_es}")  # Imprimir inmediatamente
            current_movies += 1
            new_movies_added = True  # Se han añadido nuevas películas
            last_movie_added_time = datetime.now()  # Actualizamos la hora de la última adición
            if current_movies >= max_movies:
                break
    
    # Si no se añadieron nuevas películas en 2 minutos, finalizamos
    if not new_movies_added and datetime.now() - last_movie_added_time > timeout_duration:
        print("\nNo se han añadido nuevas películas en los últimos 2 minutos. Terminando ejecución.")
        break
    
    # Hacer scroll para cargar más películas
    load_more_movies()
    
    # Actualizamos el HTML para seguir cargando más películas
    html_es = driver.page_source  # Obtenemos el HTML actual sin hacer una recarga
    soup_es = BeautifulSoup(html_es, 'html.parser')

# Generar ids únicos para las nuevas películas
new_ids = range(last_id + 1, last_id + len(movie_titles_es) + 1)

# Crear un DataFrame con los títulos nuevos y sus ids
df_new = pd.DataFrame({
    'id': new_ids,  # Asignar ids únicos continuando desde el último id
    'Titulo en Espanol': movie_titles_es
})

# Combinar los títulos nuevos con los existentes
df_combined = pd.DataFrame({
    'id': range(1, len(existing_titles) + 1),  # Generar los ids desde 1 para los títulos existentes
    'Titulo en Espanol': existing_titles
})

# Concatenar los nuevos títulos con los existentes (sin duplicados)
df_combined = pd.concat([df_combined, df_new]).drop_duplicates(subset=['Titulo en Espanol']).reset_index(drop=True)

# Exportar el DataFrame combinado a un archivo CSV
df_combined.to_csv('peliculas_netflix.csv', index=False)

# Imprimir resumen final
print("\nExtracción completa. DataFrame generado:")
print(df_combined)

# Cerrar el navegador
driver.quit()
