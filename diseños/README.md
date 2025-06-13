# Diseños del Almacén de Datos

En esta carpeta se incluyen los diseños Conceptual, Lógico y Físico de nuestro almacén de datos, basados en un **esquema estrella**.

---

## 1. Diseño Conceptual

El diseño conceptual define de forma abstracta las entidades y relaciones clave del sistema, sin entrar en tipos ni campos específicos. Para su elaboración se utilizó **draw\.io**, una herramienta online gratuita e intuitiva.

\
\
**Figura 1. Diseño conceptual (esquema estrella) – Elaboración propia**



**Elementos principales:**

- **Tabla de hechos:** `Contenido audiovisual` (central).
- **Dimensiones:**
  - **Lanzamiento**: jerarquías mes, época (primavera, verano, otoño, invierno, Navidad) y año.
  - **Género**: hasta 3 géneros (principal + 2 secundarios).
  - **Empresa productora**: empresa responsable de la producción.
  - **Idioma**: uno o varios idiomas disponibles.
  - **Tipo**: película o serie.
  - **Producción**: país de producción.
  - **Plataforma**: Netflix, Prime Video, Apple TV.
  - **Premio más importante**: galardón de mayor relevancia obtenido.
  - **Actor**, **Director**, **Escritor/Guionista**: atributos como nombre, grupo de edad, Oscar y lugar de nacimiento.

---

## 2. Diseño Lógico

El diseño lógico traduce el modelo conceptual a un esquema de entidades y relaciones detallado, incluyendo llaves y cardinalidades. Se realizó en **MySQL Workbench**.

\
\
**Figura 2. Diseño lógico – Elaboración propia**



Este modelo sirve como base para generar el script del **Diseño Físico en HIVE**.

---

## 3. Diseño Físico (Hive)

A partir del DDL generado por MySQL Workbench, se adaptó al dialecto de **Apache Hive** eliminando claves foráneas y ajustando tipos de datos:

- `VARCHAR(n)` → `STRING`
- `TINYINT` → `BOOLEAN`



> *El script completo está disponible en este repositorio junto a las imágenes de los diagramas.*

