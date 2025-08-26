import re
import logging
import pandas as pd
import numpy as np
from fuzzywuzzy import process, fuzz
from tqdm import tqdm

# ────────────────────────────────────────────────────────────────
# 📦 FUNCIONES PARA PARSEO Y NORMALIZACIÓN DE LICITACIONES
# ────────────────────────────────────────────────────────────────

def parsear_nombre_licitacion(nombre_str):
    """
    Dado un string tipo "Contratación Menor / Preadjudicación N° 401-0086-LPU25",
    extrae tipo, etapa y código de licitación. Devuelve dict con esas claves.
    """
    result = {"tipo_licitacion": None, "etapa_licitacion": None, "codigo_licitacion": None}
    if not isinstance(nombre_str, str):
        return result

    partes = re.split(r'\s*/\s*', nombre_str, maxsplit=1)
    if len(partes) < 2:
        return result

    tipo = partes[0].strip()
    match = re.search(r'(.*?)\s*N°\s*(.*)', partes[1], re.IGNORECASE)

    if match:
        etapa = match.group(1).strip()
        codigo = match.group(2).strip()
    else:
        etapa = partes[1].strip()
        codigo = None

    result["tipo_licitacion"] = tipo
    result["etapa_licitacion"] = etapa
    result["codigo_licitacion"] = codigo
    return result

def parsear_licitacion_con_apertura(nombre_str, sumario_str):
    """
    Si no se detecta la etapa de licitación desde el nombre,
    revisa el campo 'sumario' para inferir si es 'Apertura'.
    """
    base = parsear_nombre_licitacion(nombre_str)
    if not base["etapa_licitacion"] and isinstance(sumario_str, str):
        lower_sumario = sumario_str.lower()
        if "apertura:" in lower_sumario or "fecha de apertura" in lower_sumario:
            base["etapa_licitacion"] = "Apertura"
    return base

def normalizar_etapa(etapa):
    """
    Normaliza etapa de licitación a minúsculas y valores estándar.
    Devuelve None si no está mapeada.
    """
    if not etapa:
        return None

    ETAPAS_MAP = {
        "llamado": "llamado",
        "preadjudicación": "preadjudicacion",
        "preadjudicacion": "preadjudicacion",
        "adjudicación": "adjudicacion",
        "adjudicacion": "adjudicacion",
        "prórroga": "prorroga",
        "prorroga": "prorroga",
        "circular con consulta": "circular_con_consulta",
        "circular sin consulta": "circular_sin_consulta",
        "corrección": "correccion",
        "correccion": "correccion",
        "apertura": "apertura"
    }
    return ETAPAS_MAP.get(etapa.lower(), None)

def extraer_campos_licitacion(row):
    """
    Aplica parseo + normalización a una fila de DataFrame con
    campos 'nombre' y 'sumario'.
    Devuelve dict con tipo, etapa y código de licitación.
    """
    nombre = row.get("nombre", None)
    sumario = row.get("sumario", "")
    base = parsear_licitacion_con_apertura(nombre, sumario)
    base["etapa_licitacion"] = normalizar_etapa(base["etapa_licitacion"])
    return base

# ────────────────────────────────────────────────────────────────
# 🧼 LIMPIEZA DE TEXTO
# ────────────────────────────────────────────────────────────────

def limpiar_texto_licitacion(texto):
    """
    Limpia el texto de una licitación (o norma de licitación):
      - Reemplaza saltos de línea \n por espacios
      - Collapsa secuencias de espacios en un único espacio
      - Elimina espacios al inicio y final

    Retorna el texto limpio.
    """
    if not isinstance(texto, str):
        return texto  # Si no es str, no tocamos

    # Sustituir saltos de línea por espacios
    texto = texto.replace('\n', ' ')

    # Reemplazar múltiples espacios por uno
    texto = re.sub(r'\s+', ' ', texto)

    # Trimear espacios
    texto = texto.strip()

    return texto

# ────────────────────────────────────────────────────────────────
# 💵 EXTRACCIÓN DE MONTOS
# ────────────────────────────────────────────────────────────────

def extraer_monto_total(texto_limpio):
    """
    Extrae montos totales del texto limpio. Reconoce expresiones como:
    "$ 2.400.000,00", "USD 1.000.000,00", "cotiza la suma total de..." etc.
    Ignora montos que sean claramente bajos (< 100.000).
    """
    if not isinstance(texto_limpio, str):
        return 0

    # Buscar patrones con contexto semántico útil
    contexto_regex = re.compile(
        r"(USD|\$)?\s*((?:\d{1,3}(?:\.\d{3})+|\d+),\d{2})",
        flags=re.IGNORECASE
    )

    raw_montos = contexto_regex.findall(texto_limpio)
    montos_normalizados = []

    for _, monto_str in raw_montos:
        try:
            monto = float(monto_str.replace(".", "").replace(",", "."))
            if monto >= 100000:
                montos_normalizados.append(monto)
        except ValueError:
            continue

    return max(montos_normalizados) if montos_normalizados else 0




# ────────────────────────────────────────────────────────────────
# 🔍 BÚSQUEDA DE EMPRESAS
# ────────────────────────────────────────────────────────────────

def buscar_empresas_fuzzy(texto, empresas_normalizadas, threshold=85):
    """
    Aplica búsqueda fuzzy por ventana (3 a 5 palabras) sobre el texto limpio.
    Devuelve lista de coincidencias encontradas sobre empresas_normalizadas.
    """
    texto_limpio = limpiar_texto_licitacion(texto).upper()
    if len(texto_limpio) < 10:
        return []

    palabras = texto_limpio.split()
    empresas_encontradas = set()
    for window_size in range(3, 6):
        for i in range(len(palabras) - window_size + 1):
            segmento = ' '.join(palabras[i:i+window_size])
            match, score = process.extractOne(segmento, empresas_normalizadas, scorer=fuzz.token_set_ratio)
            if score >= threshold:
                empresas_encontradas.add(match)
    return list(empresas_encontradas)

def procesar_licitaciones_con_progreso(df, df_empresas, threshold=85, bloque_size=500):
    """
    Busca empresas en el texto de licitaciones usando fuzzy matching.
    Devuelve una lista de diccionarios con CUIT y nombre de la empresa encontrada.
    """
    resultados = []

    # Diccionario de búsqueda: {nombre_normalizado: cuit}
    mapa_empresas = dict(zip(df_empresas['company_name_normalized'], df_empresas['cuit_empresa']))
    empresas_normalizadas = list(mapa_empresas.keys())

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        try:
            texto = row['texto_licitaciones']
            id_licitacion = row['id_norma']
            if pd.isna(texto):
                resultados.append([])
                continue

            texto_limpio = limpiar_texto_licitacion(texto).upper()
            palabras = texto_limpio.split()
            empresas_detectadas = []

            total_bloques = len(palabras) // bloque_size + 1

            for bloque in range(total_bloques):
                inicio = bloque * bloque_size
                fin = inicio + bloque_size
                segmento = ' '.join(palabras[inicio:fin])

                match, score = process.extractOne(segmento, empresas_normalizadas, scorer=fuzz.token_set_ratio)
                if score >= threshold:
                    nombre = match
                    cuit = mapa_empresas.get(nombre)
                    empresas_detectadas.append({"cuit": cuit, "nombre": nombre})
                    logging.info(f"ID: {id_licitacion} | Empresa: {nombre} (CUIT: {cuit}) | Score: {score}")

            # Eliminar duplicados por CUIT
            empresas_unicas = {(e["cuit"], e["nombre"]) for e in empresas_detectadas if e["cuit"]}
            resultados.append([{"cuit": c, "nombre": n} for c, n in empresas_unicas])

        except Exception as e:
            logging.error(f"Error en ID {id_licitacion}: {str(e)}")
            resultados.append([])

    return resultados
