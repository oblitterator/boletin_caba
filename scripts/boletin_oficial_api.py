import pandas as pd
import pdfplumber
import requests
from datetime import datetime, timedelta
from pathlib import Path
import time
from io import BytesIO
import contextlib
import sys
import os
import warnings

class BoletinOficialAPI:
    BASE_URL = "https://api-restboletinoficial.buenosaires.gob.ar"
    PDF_DIR = Path("data/bronze/boletin_api/licitaciones_pdf")
    errores_pdf_normas = []  # ← agregar al principio de la clase

    @staticmethod
    def obtener_boletin(fecha: str, retries: int = 2, delay: float = 2.0) -> dict:
        """Obtiene boletín oficial para una fecha específica (dd-mm-yyyy)."""
        url = f"{BoletinOficialAPI.BASE_URL}/obtenerBoletin/{fecha}/true"
        for intento in range(retries + 1):
            try:
                resp = requests.get(url)
                if resp.status_code == 404:
                    return {"error": "404 Not Found", "fecha": fecha}
                resp.raise_for_status()
                return {"data": resp.json(), "fecha": fecha}
            except requests.exceptions.HTTPError as e:
                if resp.status_code >= 500:
                    print(f"⚠️ Error del servidor (HTTP {resp.status_code}) en intento {intento + 1} para {fecha}")
                    if intento < retries:
                        time.sleep(delay)
                        continue
                    return {"error": f"HTTP {resp.status_code}", "fecha": fecha}
                return {"error": f"HTTP {resp.status_code}", "fecha": fecha}
            except Exception as e:
                print(f"⚠️ Error inesperado al obtener boletín de {fecha}: {e}")
                return {"error": str(e), "fecha": fecha}
        url = f"{BoletinOficialAPI.BASE_URL}/obtenerBoletin/{fecha}/true"
        try:
            resp = requests.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code >= 500:
                print(f"⚠️ Error del servidor (HTTP {resp.status_code}) al obtener boletín de {fecha}")
                return None
            raise e  # relanza otros errores que podrían ser 403, 400
        except Exception as e:
            print(f"⚠️ Error inesperado al obtener boletín de {fecha}: {e}")
            return None

    @staticmethod
    def obtener_boletines_desde_fecha(
        fecha_inicio: str,
        boletines_existentes: set = None,
        fechas_cubiertas: set = None
    ):
        """
        Descarga boletines desde una fecha, ignorando fechas ya cubiertas o boletines ya existentes.
        También registra errores HTTP y errores del servidor en la respuesta lógica (XML no encontrado).
        """
        form = "%d-%m-%Y"
        fecha_inicio_dt = datetime.strptime(fecha_inicio, form)
        hoy = datetime.today()
        resultados = []
        errores_boletines = []
        boletines_descargados = set()
        boletines_existentes = boletines_existentes or set()
        fechas_cubiertas = fechas_cubiertas or set()

        while fecha_inicio_dt <= hoy:
            fecha_str = fecha_inicio_dt.strftime(form)

            # ⏩ Saltar fechas que ya tienen boletín registrado
            if fecha_str in fechas_cubiertas:
                print(f"⏩ Fecha {fecha_str} ya cubierta por boletín en Bronze. No se consulta API.")
                fecha_inicio_dt += timedelta(days=1)
                continue

            print(f"📥 Consultando API con fecha: {fecha_str}")
            result = BoletinOficialAPI.obtener_boletin(fecha_str)

            if result.get("data") and "boletin" in result["data"]:
                data = result["data"]
                boletin = data["boletin"]
                num_boletin = boletin.get("numero")
                fecha_pub_str = boletin.get("fecha_publicacion", fecha_str)

                # 📛 Detectar mensaje de error de servidor en normas
                normas_data = data.get("normas", {})
                if isinstance(normas_data, dict) and "errores" in normas_data:
                    mensaje = normas_data["errores"][0] if normas_data["errores"] else "Error en normas (sin mensaje)"
                    print(f"⚠️ Error de servidor en normas del boletín del {fecha_str} → {mensaje}")
                    errores_boletines.append({
                        "fecha": fecha_str,
                        "error": mensaje,
                        "tipo_error": "servidor_xml"
                    })
                    fecha_inicio_dt += timedelta(days=1)
                    continue

                if num_boletin in boletines_existentes:
                    print(f"⚠️ Boletín {num_boletin} ya estaba en Bronze. Ignorando.")
                elif num_boletin in boletines_descargados:
                    print(f"⚠️ Boletín {num_boletin} ya descargado en esta sesión. Ignorando.")
                else:
                    resultados.append({
                        "fecha_publicacion": fecha_pub_str,
                        "datos": data
                    })
                    boletines_descargados.add(num_boletin)

            else:
                tipo_error = result.get("error", "Desconocido")
                print(f"⚠️ Falló descarga para {fecha_str} → {tipo_error}")
                errores_boletines.append({
                    "fecha": fecha_str,
                    "error": tipo_error,
                    "tipo_error": "http" if "HTTP" in tipo_error or tipo_error.startswith("5") else "desconocido"
                })

            fecha_inicio_dt += timedelta(days=1)

        print(f"✅ Se descargaron {len(resultados)} boletines nuevos.")

        if errores_boletines:
            Path("logs").mkdir(exist_ok=True)
            pd.DataFrame(errores_boletines).drop_duplicates().to_csv("logs/log_errores_boletines.csv", index=False)
            print("📝 Log de errores guardado en: logs/log_errores_boletines.csv")

        return resultados


    @staticmethod
    def parsear_boletines(boletines: list) -> pd.DataFrame:
        """
        Convierte lista de boletines descargados en DataFrame limpio.
        Solo se conservan campos compatibles con Delta Lake.
        Elimina claves anidadas inesperadas como '1'.
        """
        boletin_rows = []
        for item in boletines:
            boletin = item["datos"].get("boletin", {})
            if not boletin:
                continue

            # 🧹 Eliminar claves anidadas numéricas o mal estructuradas
            boletin = {
                k: v for k, v in boletin.items()
                if not isinstance(k, str) or not k.isnumeric()
            }

            boletin["fecha_publicacion"] = item.get("fecha_publicacion")
            boletin_rows.append(boletin)

        df = pd.json_normalize(boletin_rows)

        # Convertir fechas y agregar año
        df["fecha_publicacion"] = pd.to_datetime(df["fecha_publicacion"], dayfirst=True, errors="coerce")
        df["anio"] = df["fecha_publicacion"].dt.year

        # Solo las columnas válidas esperadas por Bronze
        columnas_validas = [
            "fecha_publicacion", "mes", "dia", "anio", "numero",
            "numero2", "nombre", "url_boletin", "separata"
        ]
        df = df[[col for col in columnas_validas if col in df.columns]].copy()

        return df

    @staticmethod
    def parsear_normas(boletines: list) -> pd.DataFrame:
        """Convierte boletines a DataFrame de normas."""
        all_rows = []
        for item in boletines:
            fecha = item["fecha_publicacion"]
            normas_data = item["datos"].get("normas", {})
            
            if "errores" in normas_data:
                print(f"⚠️ Errores en normas del día {fecha}: {normas_data['errores']}")
                continue

            normas = normas_data.get("normas", {})
            for subsecciones, tipos in normas.items():
                for tipo_norma, organismos in tipos.items():
                    for organismo, lista_normas in organismos.items():
                        for norma in lista_normas:
                            all_rows.append({
                                "fecha_publicacion": fecha,
                                "subsecciones": subsecciones,
                                "tipo_norma": tipo_norma,
                                "organismo": organismo,
                                **norma
                            })

        return pd.json_normalize(all_rows)

    @staticmethod
    def descargar_pdf(id_norma: int, url_pdf: str, timeout=10) -> Path:
        """Descarga PDF localmente si no existe ya y retorna ruta."""
        BoletinOficialAPI.PDF_DIR.mkdir(parents=True, exist_ok=True)
        file_path = BoletinOficialAPI.PDF_DIR / f"{id_norma}.pdf"

        if file_path.exists():
            print(f"✅ PDF ya descargado: {file_path}")
            return file_path

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        try:
            resp = requests.get(url_pdf, headers=headers, timeout=timeout)
            resp.raise_for_status()
            file_path.write_bytes(resp.content)
            print(f"📥 PDF descargado: {file_path}")
        except requests.exceptions.Timeout:
            print(f"⚠️ Timeout al descargar PDF id_norma {id_norma}")
        except Exception as e:
            print(f"⚠️ Error al descargar PDF id_norma {id_norma}: {e}")

        time.sleep(0.2)  # Para no sobrecargar servidor
        return file_path

    @staticmethod
    def extraer_texto_pdf(path_pdf: Path) -> str:
        """Extrae y devuelve texto desde PDF, silenciando warnings de CropBox."""
        try:
            with open(os.devnull, "w") as fnull, contextlib.redirect_stdout(fnull), contextlib.redirect_stderr(fnull):
                with pdfplumber.open(path_pdf) as pdf:
                    texto = "\n".join([pag.extract_text() or "" for pag in pdf.pages])
            return texto
        except Exception as e:
            print(f"⚠️ Error al extraer texto de {path_pdf}: {e}")
            return ""
    
    @staticmethod
    def procesar_licitaciones(df_normas: pd.DataFrame) -> pd.DataFrame:
        """Procesa licitaciones: descarga PDFs y extrae textos."""

        BoletinOficialAPI.errores_pdf_normas = []  # ← limpiar errores anteriores
        df_licitaciones = df_normas[df_normas["subsecciones"] == "Licitaciones"].copy()
        textos_pdf = []
        total = len(df_licitaciones)

        for i, row in enumerate(df_licitaciones.itertuples(), 1):
            print(f"📑 Procesando PDF {i}/{total}: id_norma {row.id_norma}", flush=True)
            try:
                pdf_path = BoletinOficialAPI.descargar_pdf(row.id_norma, row.url_norma)
                if not pdf_path.exists():
                    raise FileNotFoundError(f"No se encontró el archivo para id_norma {row.id_norma}")
                texto = BoletinOficialAPI.extraer_texto_pdf(pdf_path)
            except Exception as e:
                mensaje_error = f"{e}"
                print(f"❌ Error al procesar PDF {row.id_norma}: {mensaje_error}")
                BoletinOficialAPI.errores_pdf_normas.append({
                    "id_norma": row.id_norma,
                    "error": mensaje_error
                })
                texto = ""

            textos_pdf.append(texto)

        df_licitaciones["texto_licitaciones"] = textos_pdf
        return df_licitaciones[["id_norma", "nombre", "url_norma", "texto_licitaciones", "fecha_publicacion"]]


    @staticmethod
    def obtener_organismos_emisores() -> pd.DataFrame:
        """Obtiene organismos emisores."""
        url = f"{BoletinOficialAPI.BASE_URL}/obtenerOrganismosEmisores"
        resp = requests.get(url)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())

    @staticmethod
    def obtener_reparticiones() -> pd.DataFrame:
        """Obtiene reparticiones."""
        url = f"{BoletinOficialAPI.BASE_URL}/obtenerReparticiones"
        resp = requests.get(url)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())

    @staticmethod
    def parsear_organismos_emisores(data: list) -> pd.DataFrame:
        return pd.DataFrame(data)

    @staticmethod
    def parsear_reparticiones(data: list) -> pd.DataFrame:
        return pd.DataFrame(data)
