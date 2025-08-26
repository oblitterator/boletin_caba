import re
import pandas as pd


def normalizar_sufijos(nombre):
    """
    Normaliza los sufijos comunes de nombres societarios en mayúsculas.
    """
    if not isinstance(nombre, str):
        return nombre

    sufijos = {
        r'\bS[.\s]*A[.\s]*\b': 'S.A.',
        r'\bS[.\s]*R[.\s]*L[.\s]*\b': 'S.R.L.',
        r'\bS[.\s]*A[.\s]*S[.\s]*\b': 'S.A.S.',
        r'\bC[.\s]*I[.\s]*A[.\s]*\b': 'C.I.A.',
        r'\bS[.\s]*H[.\s]*\b': 'S.H.',
        r'\bS[.\s]*E[.\s]*\b': 'S.E.',
        r'\bS[.\s]*C[.\s]*\b': 'S.C.',
        r'\bS[.\s]*E[.\s]*N[.\s]*C[.\s]*\b': 'S.E.N.C.'
    }

    nombre = re.sub(r'\s*\.\s*', '.', nombre.upper())
    for patron, reemplazo in sufijos.items():
        nombre = re.sub(patron, reemplazo, nombre, flags=re.IGNORECASE)
    nombre = re.sub(r'\.+$', '.', nombre)
    return nombre.strip()


def extraer_pais_y_cuit(company_id):
    """
    Extrae país y CUIT en formato NN-NNNNNNNN-N desde un company_id OCDS.
    """
    if not isinstance(company_id, str):
        return pd.NA, pd.NA

    match = re.match(r'^([A-Z]{2})-CUIT-([\d\-]+)-supplier$', company_id)
    if not match:
        return pd.NA, pd.NA

    pais = match.group(1)
    cuit_raw = match.group(2).replace("-", "")

    if len(cuit_raw) == 11:
        cuit_formateado = f"{cuit_raw[:2]}-{cuit_raw[2:10]}-{cuit_raw[10]}"
        return pais, cuit_formateado
    else:
        return pais, pd.NA


def generar_df_empresas_ocid(df_ocid):
    """
    A partir del DataFrame OCDS (full_ocid), extrae proveedores (-supplier), normaliza nombres,
    extrae país y CUIT, y elimina duplicados por CUIT.
    """
    indices = {
        int(m.group(1)) for col in df_ocid.columns
        if (m := re.match(r'parties/(\d+)/id', col))
    }

    empresas = []
    for i in sorted(indices):
        col_id = f'parties/{i}/id'
        col_name = f'parties/{i}/name'
        if col_id in df_ocid.columns and col_name in df_ocid.columns:
            df_temp = df_ocid[[col_id, col_name]].dropna()
            df_temp = df_temp[df_temp[col_id].str.endswith('-supplier', na=False)]
            df_temp.columns = ['company_id', 'company_name']
            empresas.append(df_temp)

    df_empresas = pd.concat(empresas).drop_duplicates().reset_index(drop=True)

    df_empresas[['pais_empresa', 'cuit_empresa']] = df_empresas['company_id'].apply(
        lambda x: pd.Series(extraer_pais_y_cuit(x))
    )

    df_empresas = df_empresas[df_empresas['cuit_empresa'].notna()]
    df_empresas = df_empresas.drop_duplicates(subset=['cuit_empresa'], keep='first')

    df_empresas['company_name_normalized'] = df_empresas['company_name'].apply(normalizar_sufijos)

    return df_empresas