import os
from deltalake import write_deltalake, DeltaTable
import pandas as pd

def upsert_data(df_nuevos, table_path, key_col, partition_col=None):
    """
    Inserta datos en Delta Lake asegurando que no haya duplicados.

    - `df_nuevos`: DataFrame Pandas con los nuevos datos a insertar.
    - `table_path`: Ruta del Delta Table.
    - `key_col`: Nombre de la columna clave única para evitar duplicados.
    - `partition_col`: (Opcional) Columna por la cual se particionará la tabla.
    """

    if df_nuevos.empty:
        print(f"⚠️ No hay nuevos registros para insertar en {table_path}.")
        return

    # ✅ Sanitizar columna clave
    if key_col in df_nuevos.columns:
        original_dtype = df_nuevos[key_col].dtype

        if pd.api.types.is_numeric_dtype(original_dtype) or original_dtype == object:
            df_nuevos[key_col] = pd.to_numeric(df_nuevos[key_col], errors="coerce").astype("Int64")
            df_nuevos = df_nuevos[df_nuevos[key_col].notna()]

        print(f"📊 Tipo de '{key_col}' después del saneo: {df_nuevos[key_col].dtype}")
        print(f"🔢 Registros con clave no nula: {df_nuevos.shape[0]}")

    try:
        # 🔹 Cargar tabla existente
        dt = DeltaTable(table_path)

        print(f"🔍 Ya hay {dt.to_pandas().shape[0]} registros en DeltaTable {table_path}.")
        print(f"🔍 Intentamos insertar {len(df_nuevos)} registros nuevos.")

        # 🔥 Merge por columna clave dinámica
        dt.alias("delta") \
            .merge(
                df_nuevos.alias("nuevos"),
                predicate=f"delta.{key_col} = nuevos.{key_col}"
            ) \
            .whenNotMatchedInsertAll() \
            .execute()

        print(f"✅ Upsert ejecutado en DeltaTable {table_path}.")
        return

    except Exception as e:
        print(f"📂 No existe DeltaTable en {table_path} o no se pudo hacer MERGE. Creando nueva tabla...")

    # ✅ Guardar como nueva tabla si no existe
    write_params = {"mode": "append"}
    if partition_col:
        write_params["partition_by"] = [partition_col]

    write_deltalake(table_path, df_nuevos, **write_params)
    print(f"✅ Datos insertados en {table_path}. Filas nuevas: {df_nuevos.shape[0]}")
