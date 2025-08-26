import pandas as pd

def limpiar_dataframe(df: pd.DataFrame, key_cols: list = None) -> pd.DataFrame:
    """
    Limpieza generalizada para DataFrames.
    - Elimina duplicados según columnas clave (opcional).
    - Normaliza columnas de tipo string (sin cambiar a mayúsculas).
    - Rellena automáticamente valores nulos según tipo de dato.

    Args:
        df (pd.DataFrame): DataFrame a limpiar.
        key_cols (list, opcional): Columnas a considerar para eliminar duplicados.

    Returns:
        pd.DataFrame: DataFrame limpio.
    """
    df = df.copy()

    # 1. Eliminar duplicados si se indican columnas clave
    if key_cols:
        df.drop_duplicates(subset=key_cols, inplace=True)

    # 2. Normalizar y limpiar columnas de tipo object (string)
    cols_str = df.select_dtypes(include=['object']).columns
    for col in cols_str:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].fillna("sin_datos")

    # 3. Rellenar columnas numéricas
    cols_num = df.select_dtypes(include=['number']).columns
    for col in cols_num:
        df[col] = df[col].fillna(-1)

    # 4. Rellenar columnas datetime
    cols_datetime = df.select_dtypes(include=['datetime']).columns
    for col in cols_datetime:
        df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))

    # 5. Rellenar columnas tipo listas/dict u otros objetos
    cols_obj = set(df.columns) - set(cols_str) - set(cols_num) - set(cols_datetime)
    for col in cols_obj:
        df[col] = df[col].apply(lambda x: x if isinstance(x, (list, dict)) else [])
    
   
    df.reset_index(drop=True)
    
    return df
