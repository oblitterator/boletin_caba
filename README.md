
---
"Extracción y Almacenamiento con API del BO de CABA - TP1"
---

# 📘 Introducción

El Boletín Oficial de la Ciudad Autónoma de Buenos Aires es una publicación oficial emitida por el Gobierno porteño que tiene validez jurídica. Publica normas, resoluciones y disposiciones administrativas. La Ciudad ofrece una API pública para acceder a esta información:  
🔗 https://api-restboletinoficial.buenosaires.gob.ar/

Mediante esta API se puede:
- Obtener el boletín oficial a partir de una fecha.
- Consultar secciones, normas y anexos publicados.
- Acceder a organismos emisores y reparticiones.

---

# 🎯 Objetivo

El proyecto tiene por finalidad extraer, transformar y almacenar datos del Boletín Oficial, especialmente normas relacionadas a **licitaciones públicas**, permitiendo consultas posteriores, enriquecimiento, y análisis textual. El procesamiento incluye:
- Descarga de boletines y normas en crudo.
- Extracción de texto de licitaciones.
- Identificación de empresas participantes.
- Cálculo de adjudicaciones.
- Enriquecimiento con organismos emisores.

---

# 📂 Estructura del Proyecto

```bash
📁 TP1_DeltaLake
│
├── 📂 data
│   ├── 📂 bronze
│   │   ├── 📂 boletines
│   │   ├── 📂 normas
│   │   ├── 📂 licitaciones_pdf
│   │   ├── 📂 organismos_emisores
│   │   ├── 📂 reparticiones
│   │   └── 📂 bac_anual
│   └── 📂 silver
│       ├── 📂 boletines
│       ├── 📂 licitaciones
│       ├── 📂 organismos_emisores
│       └── 📂 empresas
│
├── 📂 scripts
│   ├── boletin_oficial_api.py          # Extracción desde la API
│   ├── transformaciones.py             # Limpieza de boletines y normas
│   ├── transformaciones_licitaciones.py # Procesamiento de licitaciones
│   ├── transformaciones_empresas.py    # Normalización de empresas
│   ├── storage.py                      # Upserts y escritura Delta Lake
│   ├── utils_logs.py                   # Gestión de errores y logs
│
├── 📂 logs  # Logs de errores
│
├── almacenamiento.ipynb     # Script principal carga Bronze
├── procesamiento.ipynb      # Script principal carga Silver
├── requirements.txt      # Librerías necesarias
├── README.md
```

---

# 🚀 Flujo de Trabajo

## 🏗️ Almacenamiento (Bronze)

### 🔄 Extracción Incremental
- Arranca desde `fecha_inicio_actualizada` o `FECHA_INICIO` (si se fuerza manualmente).
- Consulta día a día, verifica duplicados por `numero` o `id_norma`.
- Filtra normas de licitaciones (`subsecciones == "Licitaciones"`), descarga los PDF y extrae su texto.
- Aplica `merge` en Delta Lake para evitar duplicados.
- Particiona por `anio` (boletines) y `tipo_norma` (normas).

### 📥 Extracción Full
- Descarga **organismos emisores**, **reparticiones** y **compras anuales (BAC Compras- OCID)**.
- Modo `overwrite`, sin particionado.

### 🛡️ Control de Errores
- Manejo de `timeouts`, errores HTTP y errores lógicos del servidor (e.g. XML no encontrado).
- Registro de errores en logs:
  - `logs/log_errores_boletines.csv:` errores al obtener boletines por fecha.
  - `logs/log_errores_normas.csv`: errores al descargar normas en formato PDF.
  - `logs/log_errores_normas_corregidos.csv`: errores que fueron solucionados en una corrida posterior (detectados por ID de norma).
- Se verifica automáticamente si un error previamente registrado fue corregido, y en tal caso se mueve al archivo de "corregidos".
---

# 🧠 Procesamiento (Silver)

### 1. Carga y Deduplicación
  - Se cargan y deduplican los datos crudos desde Delta Lake Bronze:
  - boletines, normas, licitaciones, organismos, reparticiones y compras OCID.
  - Se prioriza el registro más reciente según `fecha_extraccion`.

### 2. Limpieza de Licitaciones
- Se extraen `tipo_licitacion`, `etapa_licitacion`, y `codigo_licitacion`.
- Se limpian textos con reglas específicas.

### 3. Extracción de Montos
- Regex para identificar montos totales de licitación.
- Beta: Se consideran indistinamente expresiones con `$` o `USD`, y formatos como `1.000.000,00`.

### 4. Match de Empresas
- Se genera un maestro de empresas en base al dataset de BAC compras en el formato OCID por cuit, pais, y razón social (`df_empresas`).
- Se busca las empresas en el texto de las licitaciones por coincidencia de caracteres (`fuzzywuzzy`).
- Se registra la presencia de empresas por licitación en etapas de preadjudicación, adjudicación y prórroga.

### 5. Cálculo de Métricas por Empresa
- Para cada empresa se calcula:
  - `total_presentaciones`: cantidad de veces que aparece en normas de preadjudicación, adjudicación o prórrogas de licitaciones.
  - `presentaciones_adjudicacion`: cuántas veces ganó una licitación.
- Se permite clasificar y rankear según "éxito".

### 6. Enriquecimiento con Información de Normas
- Se hace merge con el número de boletín y organismo correspondiente a cada norma para asociar la norma al organismo emisor.

### 7. Análisis por Organismo
- Se genera un resumen por `organismo`:
  - `monto_total_adjudicado`: suma de todos los montos adjudicados.
  - `cantidad_empresas`: cantidad de empresas adjudicadas.

### 8. Enriquecimiento de Empresas
- A cada empresa se le agrega:
  - `organismo_top_adjudicacion`: organismo donde más fue adjudicada.
  - `organismo_top_presencia`: donde más se presentó.

### 9. Almacenamiento Final
- Los datasets enriquecidos se almacenan en Silver en formato Delta Lake  (modo `overwrite`):
  - `boletines`, `licitaciones`, `organismos_emisores`, `empresas`.
---

# 🔧 Modularización del Proyecto

- `boletin_oficial_api.py`: lógica de conexión, parseo y descarga.
- `storage.py`: inserción en Delta Lake (upsert, merge, overwrite).
- `transformaciones.py`: limpieza básica, relleno de nulos, conversión de tipos.
- `transformaciones_licitaciones.py`: extracción de campos clave, texto y montos.
- `transformaciones_empresas.py`: generación y enriquecimiento de empresas.
- `utils_logs.py`: centraliza guardado de errores en logs.

Los notebooks `almacenamiento.ipynb` y `procesamiento.ipynb` son los puntos de entrada al pipeline.


---
