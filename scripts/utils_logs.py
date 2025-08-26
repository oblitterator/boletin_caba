import pandas as pd
from pathlib import Path
from datetime import datetime
import os

def detectar_fechas_cubiertas_boletines(df_bronze):
    """
    Extrae fechas cubiertas desde boletines ya guardados en Bronze.
    Retorna set de strings en formato dd-mm-yyyy.
    """
    return set(
        pd.to_datetime(df_bronze['fecha_publicacion'], errors='coerce')
        .dropna()
        .dt.strftime("%d-%m-%Y")
        .unique()
    )


def gestionar_logs_de_errores_boletines(fechas_cubiertas, log_path="logs/log_errores_boletines.csv"):
    """
    Verifica si fechas con errores anteriores ya fueron solucionadas.
    Si se resolvieron, las mueve a log de corregidos.
    Retorna fechas de errores no resueltos como DataFrame (o None si no hay log).
    """
    corregidos_path = "logs/log_errores_corregidos.csv"

    if not os.path.exists(log_path):
        return None  # nada que hacer

    df_errores_previos = pd.read_csv(log_path)

    fechas_resueltas = df_errores_previos[
        df_errores_previos["fecha"].isin(fechas_cubiertas)
    ].copy()

    fechas_no_resueltas = df_errores_previos[
        ~df_errores_previos["fecha"].isin(fechas_cubiertas)
    ].copy()

    if not fechas_resueltas.empty:
        print("✅ Se detectaron errores anteriores que fueron resueltos:")
        for fila in fechas_resueltas.to_dict(orient="records"):
            print(f"✔️ {fila['fecha']} ({fila['error']})")

        # Agregar fecha de corrección
        fecha_corrida = datetime.now().strftime("%Y-%m-%d")
        fechas_resueltas["fecha_correccion"] = fecha_corrida

        if os.path.exists(corregidos_path):
            df_corr_prev = pd.read_csv(corregidos_path)
            df_corr = pd.concat([df_corr_prev, fechas_resueltas], ignore_index=True).drop_duplicates()
        else:
            df_corr = fechas_resueltas

        Path("logs").mkdir(exist_ok=True)
        df_corr.to_csv(corregidos_path, index=False)

    # Guardar el nuevo log de errores no resueltos
    fechas_no_resueltas.to_csv(log_path, index=False)

    return fechas_no_resueltas


def gestionar_log_errores_normas(df_normas_actual: pd.DataFrame,
                                  path_errores: str = "logs/log_errores_normas.csv",
                                  path_corregidos: str = "logs/log_errores_normas_corregidos.csv"):
    """
    Revisa si errores anteriores de descarga de normas ya se resolvieron (por id_norma).
    Limpia el log original y guarda los corregidos aparte.
    """
    if not Path(path_errores).exists():
        print("ℹ️ No existe log de errores anteriores de normas.")
        return

    df_errores_previos = pd.read_csv(path_errores)

    # 🧼 Convertir a int para evitar problemas de comparación
    df_errores_previos["id_norma"] = df_errores_previos["id_norma"].astype("Int64")
    id_normas_actuales = set(df_normas_actual["id_norma"].dropna().astype("Int64").unique())

    # ✅ Detectar corregidos
    errores_corregidos = df_errores_previos[df_errores_previos["id_norma"].isin(id_normas_actuales)].copy()
    errores_pendientes = df_errores_previos[~df_errores_previos["id_norma"].isin(id_normas_actuales)].copy()

    if not errores_corregidos.empty:
        print("✅ Se detectaron normas previamente fallidas que ya fueron procesadas:")
        for fila in errores_corregidos.to_dict(orient="records"):
            print(f"✔️ id_norma {fila['id_norma']} ({fila['error']})")

        errores_corregidos["fecha_correccion"] = datetime.now().strftime("%Y-%m-%d")

        # Guardar corregidos (append)
        if Path(path_corregidos).exists():
            df_corr_prev = pd.read_csv(path_corregidos)
            df_total = pd.concat([df_corr_prev, errores_corregidos], ignore_index=True).drop_duplicates()
        else:
            df_total = errores_corregidos

        df_total.to_csv(path_corregidos, index=False)
        print(f"📝 Log actualizado: {path_corregidos}")

    # 🧾 Guardar los errores que siguen pendientes
    errores_pendientes.to_csv(path_errores, index=False)
    if not errores_pendientes.empty:
        print(f"📌 Errores aún pendientes guardados en: {path_errores}")
    else:
        print("🎉 Todos los errores anteriores fueron corregidos.")




def guardar_log_errores_normas(lista_errores, path="logs/log_errores_normas.csv"):
    """
    Guarda un log de errores de descarga de normas (PDFs).
    Cada error debe ser un dict con las claves: 'id_norma', 'error'
    Agrega fecha y tipo de error.
    """
    if not lista_errores:
        print("ℹ️ No hay errores nuevos de normas para guardar.")
        return

    Path("logs").mkdir(exist_ok=True)
    hoy = datetime.now().strftime("%Y-%m-%d")

    # Crear DataFrame nuevo
    df_nuevo = pd.DataFrame(lista_errores)
    df_nuevo["fecha_ejecucion"] = hoy
    df_nuevo["tipo_error"] = "timeout"
    df_nuevo["id_norma"] = df_nuevo["id_norma"].astype("Int64")

    if Path(path).exists():
        df_existente = pd.read_csv(path)
        df_existente["id_norma"] = df_existente["id_norma"].astype("Int64")

        # Evita duplicados
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True).drop_duplicates(subset=["id_norma", "error"])
    else:
        df_final = df_nuevo

    df_final.to_csv(path, index=False)
    print(f"📝 Log de errores de normas guardado en: {path}")
