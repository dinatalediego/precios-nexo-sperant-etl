# -*- coding: utf-8 -*-
"""
Pipeline robusto para listas de precios (.xls antiguos) + Sperant.

Flujo:
1) Leer .xls (tolerante) desde carpeta base -> dfs por proyecto
2) Unificar en df_total (agrega 'Proyecto')
3) Normalizar claves y LEFT JOIN con df_sperant
4) Reglas de actualización ('Precio de lista', 'Estado de inmueble')
5) Exportar un .xlsx por proyecto a OUT_DIR
6) Auditoría: resumen por proyecto + detalle de cambios

Hardening incluido:
- Detección automática de header (primeras 15 filas)
- Alias → canónicos (Número/Precio/Estado)
- Coerción numérica robusta (miles/decimales mixtos)
- Resolución de duplicados Sperant (por fecha o último)
- Modo de actualización configurable
- Blindaje contra columnas duplicadas (Series garantizadas)
"""

import os
import re
import xlrd
import pandas as pd
import numpy as np
from typing import Dict, List, Optional

# =========================
# 0) Parámetros
# =========================
Carpeta   = "data_actual_en_nexo"
Carpeta_sperant   = "data_sperant_actual"                  # carpeta origen .xls
OUT_DIR   = "tablas_actualizadas/"     # carpeta destino
AUD_DIR   = os.path.join(OUT_DIR, "Auditoria")

df_sperant = pd.read_excel(Carpeta_sperant + "/BD_SPERANT_ACTUAL.xlsx")
print(f'Shape de Sperant: {df_sperant.shape}')

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(AUD_DIR, exist_ok=True)

# Archivos fuente (.xls antiguos)
archivos = {
    "Alicanto":       os.path.join(Carpeta, "Alicanto.xls"),
    "Capadocia":      os.path.join(Carpeta, "Capadocia.xls"),
    "Fenix":          os.path.join(Carpeta, "Fenix.xls"),
    "Napoles":        os.path.join(Carpeta, "Napoles.xls"),
    "Tizon_y_Bueno":  os.path.join(Carpeta, "Tizon y Bueno.xls"),
    "Sialia":         os.path.join(Carpeta, "Sialia.xls"),
    "Modena":         os.path.join(Carpeta, "Modena.xls"),
    "Matera":         os.path.join(Carpeta, "Matera.xls"),
}

# Columnas objetivo en los Nexos
COL_OBJ_NUMERO   = "Número de inmueble"
COL_OBJ_PRECIO   = "Precio de lista"
COL_OBJ_ESTADO   = "Estado de inmueble"

# Aliases posibles
ALIASES_NUMERO = [
    "Número de inmueble","Numero de inmueble","N° inmueble","N° de inmueble",
    "numero","número","nombre","nombre inmueble","unidad","codigo","código","Código de inmueble"
]
ALIASES_PRECIO = ["Precio de lista","precio de lista","precio","precio lista","Precio Lista"]
ALIASES_ESTADO = ["Estado de inmueble","estado de inmueble","estado","estado comercial"]

# Sperant esperado y reglas
SPERANT_COLS = ["nombre_proyecto","nombre","precio_lista","estado_comercial","fecha_actualizacion"]
REGLA_DUPLICADOS_SPERANT = "max_fecha"      # "max_fecha" | "ultimo"
MODO_ACTUALIZACION       = "preferir_nuevo" # "preferir_nuevo" | "preferir_existente"


# =========================
# 0.b) Utilidades
# =========================
def _norm(x):
    #str->strip->lower (para claves).
    return str(x).strip().lower() if pd.notna(x) else x

def _clean_cols(cols: List[str]) -> List[str]:
    #Limpia encabezados: strip y colapsa espacios
    return [re.sub(r"\s+", " ", str(c)).strip() for c in cols]

def _find_header_row(data: List[List[object]], candidates: List[str], max_scan: int = 15) -> Optional[int]:
    #Detecta índice de header buscando candidatos en primeras filas.
    cand_norm = set(_norm(c) for c in candidates)
    for i in range(min(max_scan, len(data))):
        row = [_norm(x) for x in data[i]]
        if any(cell in cand_norm for cell in row):
            return i
    return None

def _alias_pick(cols: List[str], aliases: List[str], canonical: str) -> str:
    #Elige el primer alias presente; si no, devuelve el canónico.
    colset = set(cols)
    for a in aliases:
        if a in colset:
            return a
    return canonical

def _safe_filename(name: str) -> str:
    return (re.sub(r"[^A-Za-z0-9._ \-\(\)]", "", str(name)).strip() or "Proyecto")

def _to_number(s):
    #Convierte strings con separadores mixtos a float.
    if pd.isna(s):
        return np.nan
    if isinstance(s, (int, float, np.number)):
        return float(s)
    txt = str(s).strip().replace(" ", "")
    if txt == "":
        return np.nan
    if "," in txt and "." in txt:
        if txt.rfind(",") > txt.rfind("."):
            txt = txt.replace(".", "").replace(",", ".")
        else:
            txt = txt.replace(",", "")
    elif "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    else:
        parts = txt.split(".")
        if len(parts) > 2:
            dec = parts[-1]; ent = "".join(parts[:-1])
            txt = f"{ent}.{dec}"
    try:
        return float(txt)
    except Exception:
        return np.nan

def _coerce_price_col(sr: pd.Series) -> pd.Series:
    return sr.map(_to_number)

def _resolve_duplicates_sperant(df: pd.DataFrame) -> pd.DataFrame:
    #Dedup por (_key_proy,_key_num) según regla.
    if REGLA_DUPLICADOS_SPERANT == "max_fecha" and "fecha_actualizacion" in df.columns:
        df = df.copy()
        df["_fecha_tmp"] = pd.to_datetime(df["fecha_actualizacion"], errors="coerce")
        df = df.sort_values(["_key_proy","_key_num","_fecha_tmp"])
        out = df.drop_duplicates(["_key_proy","_key_num"], keep="last")
        return out.drop(columns=["_fecha_tmp"])
    return df.drop_duplicates(["_key_proy","_key_num"], keep="last")

def _apply_update(existing: pd.Series, new: pd.Series) -> pd.Series:
    #Aplica regla de actualización.
    if MODO_ACTUALIZACION == "preferir_existente":
        return existing.where(existing.notna(), new)
    return new.where(new.notna(), existing)

def _get_series(df: pd.DataFrame, col: str) -> pd.Series:
    
    #Garantiza Series (no DataFrame) aún si existen columnas duplicadas con el mismo nombre.
    #Si no existe, devuelve Serie NA del largo de df.
    
    if col not in df.columns:
        return pd.Series([pd.NA]*len(df), index=df.index)
    val = df[col]
    if isinstance(val, pd.DataFrame):
        # varias columnas con el mismo nombre -> tomar la primera no nula por fila
        val = val.bfill(axis=1).iloc[:, 0]
    return val.squeeze()

def _select_series(df: pd.DataFrame, colname: str) -> pd.Series:
    #Como _get_series pero seleccionando por máscara de nombres duplicados.
    mask = (df.columns == colname)
    if mask.any():
        cols = df.loc[:, mask]
        if cols.shape[1] == 1:
            return cols.iloc[:, 0]
        return cols.bfill(axis=1).iloc[:, 0]
    return pd.Series([pd.NA]*len(df), index=df.index)

def _collapse_duplicate_name(df: pd.DataFrame, colname: str) -> pd.DataFrame:
    #Colapsa columnas duplicadas con el mismo nombre en una sola (primera no nula por fila).
    mask = (df.columns == colname)
    if mask.sum() > 1:
        cols = df.loc[:, mask]
        df = df.drop(columns=cols.columns)
        df[colname] = cols.bfill(axis=1).iloc[:, 0]
    return df


# ===================================
# 1) Lectura tolerante de .xls
# ===================================
dfs: Dict[str, pd.DataFrame] = {}
for proyecto, path in archivos.items():
    if not os.path.exists(path):
        print(f"❌ No encontrado: {path}")
        continue
    try:
        book = xlrd.open_workbook(path, logfile=open(os.devnull, "w"), ignore_workbook_corruption=True)
        sheet = book.sheet_by_index(0)
        data  = [sheet.row_values(r) for r in range(sheet.nrows)]

        header_candidates = ALIASES_NUMERO + ALIASES_PRECIO + ALIASES_ESTADO
        hdr_idx = _find_header_row(data, header_candidates, max_scan=15)
        if hdr_idx is None:
            hdr_idx = 4  # fallback a fila 5 como en tu versión inicial

        header = _clean_cols([str(x) for x in data[hdr_idx]])
        rows   = data[hdr_idx+1:]
        df = pd.DataFrame(rows, columns=header)
        df.columns = _clean_cols(df.columns.tolist())

        # Map alias -> canónico
        col_num = _alias_pick(df.columns.tolist(), ALIASES_NUMERO, COL_OBJ_NUMERO)
        col_pre = _alias_pick(df.columns.tolist(), ALIASES_PRECIO, COL_OBJ_PRECIO)
        col_est = _alias_pick(df.columns.tolist(), ALIASES_ESTADO, COL_OBJ_ESTADO)
        ren = {}
        if col_num != COL_OBJ_NUMERO: ren[col_num] = COL_OBJ_NUMERO
        if col_pre != COL_OBJ_PRECIO: ren[col_pre] = COL_OBJ_PRECIO
        if col_est != COL_OBJ_ESTADO: ren[col_est] = COL_OBJ_ESTADO
        if ren: df = df.rename(columns=ren)

        # Garantiza columnas objetivo
        for c in [COL_OBJ_NUMERO, COL_OBJ_PRECIO, COL_OBJ_ESTADO]:
            if c not in df.columns: df[c] = pd.NA

        # Coerción numérica
        df[COL_OBJ_PRECIO] = _coerce_price_col(df[COL_OBJ_PRECIO])

        # Agrega proyecto
        df.insert(0, "Proyecto", proyecto)
        dfs[proyecto] = df
        print(f"✅ {proyecto} leído: {len(df)} filas, {len(df.columns)} columnas (header en fila {hdr_idx+1})")
    except Exception as e:
        print(f"⚠️ Error leyendo {proyecto}: {e}")

if not dfs:
    raise RuntimeError("No se leyó ningún .xls. Verifica rutas y archivos.")

# ===========================
# 2) Unificar en df_total
# ===========================
df_total = pd.concat(dfs.values(), ignore_index=True)
df_total["Proyecto"] = df_total["Proyecto"].astype(str)
df_total[COL_OBJ_NUMERO] = df_total[COL_OBJ_NUMERO].astype(str)
for c in [COL_OBJ_PRECIO, COL_OBJ_ESTADO]:
    if c not in df_total.columns:
        df_total[c] = pd.NA 

# 🔹 Conversión segura: 'Número de inmueble' → string sin decimales
if "Número de inmueble" in df_total.columns:
    df_total["Número de inmueble"] = (
        df_total["Número de inmueble"]
        .apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else str(x).strip())
    )

# Asegura tipo string explícito
df_total["Número de inmueble"] = df_total["Número de inmueble"].astype(str).str.strip()

# ===========================
# 2.5) Añadir el valor de Torre
# ===========================

# --- 2.c) Prefijar torre (A/B) al Nº de inmueble para Capadocia, Matera y Napoles ---
proys_con_torre = {"capadocia", "matera", "napoles"}

# máscara por proyecto (case-insensitive; trim)
mask_proy = (
    df_total.get("Proyecto", pd.Series([], dtype=str))
            .astype(str).str.strip().str.lower()
            .isin(proys_con_torre)
)

if mask_proy.any():
    if "Tipología" not in df_total.columns:
        print("⚠️ Advertencia: no existe la columna 'Tipología'; no se puede prefijar A/B.")
    else:
        # Primera letra de Tipología (A/B) en mayúscula
        tip_letra = (
            df_total.loc[mask_proy, "Tipología"]
                    .astype(str).str.strip().str[0].str.upper()
        )
        # Solo aceptamos A o B; el resto se marca como NA (no cambia)
        tip_letra = tip_letra.where(tip_letra.isin(["A", "B"]), pd.NA)

        # Nº de inmueble actual (texto)
        num_str = df_total.loc[mask_proy, "Número de inmueble"].astype(str).str.strip()

        # Evitar duplicar si ya empieza con A/B + dígitos (ej. "A101")
        ya_tiene_prefijo = num_str.str.upper().str.match(r"^[AB]\d+$")

        # Nuevo valor: si ya tiene prefijo, se deja igual; si NO y hay letra válida, se antepone
        nuevo_num = np.where(
            ya_tiene_prefijo | tip_letra.isna(),
            num_str,
            tip_letra.fillna("") + num_str
        )

        df_total.loc[mask_proy, "Número de inmueble"] = nuevo_num

        # (Opcional) validación rápida
        # print(df_total.loc[mask_proy, ["Proyecto","Tipología","Número de inmueble"]].head(15))

# (el resto de tu código sigue igual)
df_total["Proyecto"] = df_total["Proyecto"].astype(str)
for c in [COL_OBJ_PRECIO, COL_OBJ_ESTADO]:
    if c not in df_total.columns:
        df_total[c] = pd.NA

# =========================================
# 3) Cargar df_sperant (si no existe en memoria se crea vacío)
# =========================================
try:
    df_sperant = df_sperant.copy()
except NameError:
    df_sperant = pd.DataFrame(columns=SPERANT_COLS)
    print("ℹ️ Aviso: df_sperant no existía; se creó vacío (no habrá actualizaciones).")

for c in ["nombre_proyecto","nombre"]:
    if c not in df_sperant.columns: df_sperant[c] = pd.NA
for c in ["precio_lista","estado_comercial"]:
    if c not in df_sperant.columns: df_sperant[c] = pd.NA
if "fecha_actualizacion" not in df_sperant.columns:
    df_sperant["fecha_actualizacion"] = pd.NaT

df_sperant["nombre_proyecto"] = df_sperant["nombre_proyecto"].astype(str)
df_sperant["nombre"] = df_sperant["nombre"].astype(str)

# =========================================
# 4) Normalizar claves y merge
# =========================================
df_total = df_total.copy()
df_total["_key_proy"] = df_total["Proyecto"].map(_norm)
df_total["_key_num"]  = df_total[COL_OBJ_NUMERO].map(_norm)

df_sperant = df_sperant.copy()
df_sperant["_key_proy"] = df_sperant["nombre_proyecto"].map(_norm)
df_sperant["_key_num"]  = df_sperant["nombre"].map(_norm)

right = _resolve_duplicates_sperant(
    df_sperant[["_key_proy","_key_num","precio_lista","estado_comercial","fecha_actualizacion"]]
)

df_total["_rowid"] = range(len(df_total))

# =========================================
# 4.4) Previo al Merge
# =========================================
# ===== Diagnóstico de claves (ANTES DEL MERGE) =====
print("🔎 Diagnóstico de claves antes del merge:")

# Muestra 10 proyectos únicos de cada lado
print("Proyectos en df_total (Nexo) →", df_total["Proyecto"].dropna().astype(str).str.strip().str.lower().nunique())
print(df_total["Proyecto"].dropna().astype(str).str.strip().str.lower().value_counts().head(10))

if "nombre_proyecto" in df_sperant.columns:
    print("\nProyectos en df_sperant →", df_sperant["nombre_proyecto"].dropna().astype(str).str.strip().str.lower().nunique())
    print(df_sperant["nombre_proyecto"].dropna().astype(str).str.strip().str.lower().value_counts().head(10))
else:
    print("\n⚠️ df_sperant no tiene 'nombre_proyecto'")

# Chequeo de columnas de número/nombre de unidad
if "Número de inmueble" not in df_total.columns:
    print("⚠️ Nexo no trae 'Número de inmueble' (ver alias)")
if "nombre" not in df_sperant.columns:
    print("⚠️ Sperant no trae 'nombre' (ver columna equivalente a unidad/inmueble)")

# Top 20 claves crudas comparadas
print("\nEjemplos de claves Nexo →")
print(df_total[[ "Proyecto", "Número de inmueble" ]].head(20))
print("\nEjemplos de claves Sperant →")
print(df_sperant[[ "nombre_proyecto", "nombre" ]].head(20))



merged = df_total.merge(
    right, how="left", on=["_key_proy","_key_num"], suffixes=("","_new")
)

# Blindaje por si quedaron columnas duplicadas con mismo nombre
merged = _collapse_duplicate_name(merged, COL_OBJ_PRECIO)
merged = _collapse_duplicate_name(merged, COL_OBJ_ESTADO)

# =========================================
# 5) Actualizar columnas objetivo (reglas) – usando Series garantizadas
# =========================================
if "precio_lista" in merged.columns:
    existing = _get_series(merged, COL_OBJ_PRECIO)
    new_val  = _get_series(merged, "precio_lista")
    merged[COL_OBJ_PRECIO] = _apply_update(
        existing=pd.to_numeric(existing, errors="coerce"),
        new=pd.to_numeric(new_val,  errors="coerce"),
    )

if "estado_comercial" in merged.columns:
    existing = _get_series(merged, COL_OBJ_ESTADO)
    new_val  = _get_series(merged, "estado_comercial")
    merged[COL_OBJ_ESTADO] = _apply_update(existing=existing, new=new_val)

# Limpieza columnas auxiliares del right
for c in ["precio_lista","estado_comercial"]:
    if c in merged.columns: merged = merged.drop(columns=c)

# =========================================
# 6) Exportar un Excel por proyecto
# =========================================
for proyecto, dfp in merged.groupby("Proyecto", dropna=False):
    fname = _safe_filename(proyecto)
    out_path = os.path.join(OUT_DIR, f"{fname}.xlsx")
    cols_order = [c for c in ["Proyecto", COL_OBJ_NUMERO, COL_OBJ_PRECIO, COL_OBJ_ESTADO] if c in dfp.columns]
    other_cols = [c for c in dfp.columns if c not in cols_order]
    dfp = dfp[cols_order + other_cols]
    dfp.to_excel(out_path, index=False)
    print(f"📤 Exportado: {out_path} ({len(dfp)} filas)")

# =========================================
# 7) Auditoría de cambios (before vs after)
# =========================================
cols_base = ["Proyecto", COL_OBJ_NUMERO, COL_OBJ_PRECIO, COL_OBJ_ESTADO]
before = df_total[["_rowid"] + cols_base].set_index("_rowid").copy()
after  = merged  [["_rowid"] + cols_base + ["_key_proy","_key_num"]].set_index("_rowid").copy()

# Colapsar posibles duplicados de nombre en before/after
before = _collapse_duplicate_name(before, COL_OBJ_PRECIO)
before = _collapse_duplicate_name(before, COL_OBJ_ESTADO)
after  = _collapse_duplicate_name(after,  COL_OBJ_PRECIO)
after  = _collapse_duplicate_name(after,  COL_OBJ_ESTADO)

# Match con Sperant
sperant_keys = set(map(tuple, right[["_key_proy","_key_num"]].dropna().values.tolist()))
after["tiene_match"] = [ (k in sperant_keys) for k in zip(after["_key_proy"], after["_key_num"]) ]
after["Sin_Match"] = ~after["tiene_match"]

# Comparaciones (precio con tolerancia)
before["_precio_num"] = _select_series(before, COL_OBJ_PRECIO).map(_to_number)
after ["_precio_num"] = _select_series(after,  COL_OBJ_PRECIO).map(_to_number)

precio_diff = ~np.isclose(
    before["_precio_num"].astype(float),
    after["_precio_num"].astype(float),
    equal_nan=True
)
estado_diff = ( _select_series(before, COL_OBJ_ESTADO).fillna("__NA__")
              != _select_series(after,  COL_OBJ_ESTADO).fillna("__NA__") )

cambio_precio = pd.Series(precio_diff, index=after.index)
cambio_estado = pd.Series(estado_diff, index=after.index)

cmp = after[cols_base].copy()
cmp["Cambio_PrecioLista"] = cambio_precio.fillna(False)
cmp["Cambio_Estado"]      = cambio_estado.fillna(False)
cmp["Algún_Cambio"]       = cmp["Cambio_PrecioLista"] | cmp["Cambio_Estado"]
cmp["Sin_Cambio"]         = (~cmp["Algún_Cambio"]) & after["tiene_match"]
#cmp["Sin_Match"]          = ~after["tiene_match"]

# Resumen por proyecto
resumen = (
    pd.concat([cmp, after[["tiene_match","Sin_Match"]]], axis=1)
      .groupby("Proyecto", dropna=False)
      .agg(
          Registros=(COL_OBJ_NUMERO, "size"),
          Con_Match=("tiene_match", "sum"),
          Sin_Match=("Sin_Match", "sum"),
          Cambios=("Algún_Cambio", "sum"),
          Cambios_Precio=("Cambio_PrecioLista", "sum"),
          Cambios_Estado=("Cambio_Estado", "sum"),
          Sin_Cambio=("Sin_Cambio", "sum"),
      )
      .reset_index()
)
for col in ["Con_Match","Sin_Match","Cambios","Sin_Cambio"]:
    resumen[f"%_{col}"] = (resumen[col] / resumen["Registros"]).round(4)

# Proyectos presentes en un lado y no en el otro
proy_en_total   = set(df_total["Proyecto"].astype(str).str.strip())
proy_en_sperant = set(df_sperant["nombre_proyecto"].astype(str).str.strip())
proyectos_solo_total   = sorted(list(proy_en_total - proy_en_sperant))
proyectos_solo_sperant = sorted(list(proy_en_sperant - proy_en_total))

# Detalle de cambios (solo filas que cambiaron)
detalle_cambios = (
    before.reset_index()
          .merge(after.reset_index(), on=["_rowid","Proyecto", COL_OBJ_NUMERO], suffixes=("_Antes","_Despues"))
)
detalle_cambios["Cambio_PrecioLista"] = cambio_precio.reset_index(drop=True)
detalle_cambios["Cambio_Estado"]      = cambio_estado.reset_index(drop=True)
detalle_cambios = detalle_cambios.query("Cambio_PrecioLista or Cambio_Estado")

# Exportar auditoría
resumen_path = os.path.join(AUD_DIR, "Resumen_cambios_por_proyecto.xlsx")
with pd.ExcelWriter(resumen_path, engine="xlsxwriter") as xw:
    resumen.to_excel(xw, sheet_name="Resumen", index=False)
    pd.DataFrame({"Proyecto_solo_df_total": proyectos_solo_total}).to_excel(xw, sheet_name="Solo_en_df_total", index=False)
    pd.DataFrame({"Proyecto_solo_df_sperant": proyectos_solo_sperant}).to_excel(xw, sheet_name="Solo_en_sperant", index=False)

print(f"✅ Auditoría general: {resumen_path}")

# Detalles por proyecto (solo con cambios)
for proy, dfx in detalle_cambios.groupby("Proyecto", dropna=False):
    if len(dfx) == 0:
        continue
    out_p = os.path.join(AUD_DIR, f"Detalle_cambios_{_safe_filename(proy)}.xlsx")
    
    dfx.to_excel(out_p, index=False)
    print(f"📝 Detalle de cambios: {out_p}")

print("🏁 Proceso completo.")
