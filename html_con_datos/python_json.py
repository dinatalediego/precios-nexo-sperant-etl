# nexo_to_json.py
import xlrd, pandas as pd, os, json, math, datetime as dt
from pathlib import Path

# -------- Config ----------
CARPETA = "Nexo - Lista de Precios/"
ARCHIVOS = {
    "Alicanto": "Alicanto.xls",
    "Capadocia": "Capadocia.xls",
    "Fenix": "Fenix.xls",
    "Napoles": "Napoles.xls",
    "Tizon_y_Bueno": "Tizon y Bueno.xls",
    "Sialia": "Sialia.xls",
    "Modena": "Modena.xls",
    "Matera": "Matera (3).xls",
}
# Fila base: cabeceras en fila 5 (índice 4), datos desde fila 6 (índice 5)
HEADER_ROW_IDX = 4
DATA_START_IDX = 5

# Columnas objetivo (se toman si existen)
OBJ = {
    "Proyecto": "Proyecto",
    "Número de inmueble": "Numero de inmueble",
    "Numero de inmueble": "Numero de inmueble",
    "Código de inmueble": "Numero de inmueble",
    "Precio de lista": "Precio de lista",
    "Estado de inmueble": "Estado de inmueble",
    "Tipología": "Tipologia",
    "Área techada": "Area techada",
    "Área total": "Area total",
    "Dormitorios": "Dormitorios",
    "Piso": "Piso",
}

SALIDA_RECORDS = "records.json"
SALIDA_KPIS = "kpis.json"
# --------------------------

def _strip_cols(cols):
    cleaned = []
    seen = set()
    for c in cols:
        name = (str(c).strip()
                .replace("\n", " ")
                .replace("  ", " ")
                .replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
                ).strip()
        # Evitar vacíos/duplicados
        if not name:
            name = "col_sin_nombre"
        if name in seen:
            # desambiguar
            k = 2
            cand = f"{name}_{k}"
            while cand in seen:
                k += 1
                cand = f"{name}_{k}"
            name = cand
        seen.add(name)
        cleaned.append(name)
    return cleaned

def _safe_num(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    s = str(x).strip()
    if s in ("", "NA", "N/A", "#N/A", "#ND", "None", "null", "-"):
        return None
    s = s.replace(".", "").replace(",", ".")  # 1.234.567,89 -> 1234567.89
    try:
        return float(s)
    except Exception:
        return None

def _export_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def leer_xls_tolerante(path_xls: str):
    book = xlrd.open_workbook(
        path_xls,
        logfile=open(os.devnull, "w"),
        ignore_workbook_corruption=True
    )
    sheet = book.sheet_by_index(0)
    data = [sheet.row_values(r) for r in range(sheet.nrows)]
    headers = _strip_cols(data[HEADER_ROW_IDX])
    rows = data[DATA_START_IDX:]
    df = pd.DataFrame(rows, columns=headers)
    return df

def estandarizar(df: pd.DataFrame, proyecto: str) -> pd.DataFrame:
    # Dejar solo primeras ocurrencias de columnas duplicadas
    df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    # Seleccionar columnas objetivo si existen
    cols_presentes = {}
    for k_original, k_std in OBJ.items():
        if k_original in df.columns:
            cols_presentes[k_original] = k_std
    if "Proyecto" not in cols_presentes.values():
        df["Proyecto"] = proyecto

    # Renombrar a estándar
    rename_map = {orig: std for orig, std in cols_presentes.items()}
    df = df.rename(columns=rename_map)

    # Subset ordenado (manteniendo las que existen)
    orden = ["Proyecto", "Numero de inmueble", "Precio de lista", "Estado de inmueble",
             "Tipologia", "Area techada", "Area total", "Dormitorios", "Piso"]
    cols_finales = [c for c in orden if c in df.columns]
    df = df[cols_finales + [c for c in df.columns if c not in cols_finales]]

    # Limpiezas numéricas básicas
    if "Precio de lista" in df.columns:
        df["Precio de lista_num"] = df["Precio de lista"].map(_safe_num)
    if "Area techada" in df.columns:
        df["Area techada_num"] = df["Area techada"].map(_safe_num)
    if "Area total" in df.columns:
        df["Area total_num"] = df["Area total"].map(_safe_num)
    if "Dormitorios" in df.columns:
        df["Dormitorios_num"] = df["Dormitorios"].map(_safe_num)

    # Quitar filas completamente vacías
    df = df.dropna(how="all")
    return df

def kpis(df_total: pd.DataFrame):
    now = dt.datetime.now().isoformat(timespec="seconds")
    out = {"generated_at": now, "cards": {}, "by_proyecto": {}, "by_estado": {}}

    # Cards globales
    out["cards"]["unidades_totales"] = int(len(df_total))
    if "Precio de lista_num" in df_total.columns:
        out["cards"]["precio_promedio"] = round(
            float(df_total["Precio de lista_num"].dropna().mean()) if df_total["Precio de lista_num"].notna().any() else 0.0, 2
        )
        out["cards"]["precio_median"] = round(
            float(df_total["Precio de lista_num"].dropna().median()) if df_total["Precio de lista_num"].notna().any() else 0.0, 2
        )

    # KPIs por Proyecto
    if "Proyecto" in df_total.columns:
        for p, g in df_total.groupby("Proyecto"):
            rec = {"unidades": int(len(g))}
            if "Precio de lista_num" in g.columns and g["Precio de lista_num"].notna().any():
                rec["precio_promedio"] = round(float(g["Precio de lista_num"].mean()), 2)
                rec["precio_median"] = round(float(g["Precio de lista_num"].median()), 2)
            if "Estado de inmueble" in g.columns:
                rec["por_estado"] = (
                    g["Estado de inmueble"].fillna("__NA__").value_counts().to_dict()
                )
            out["by_proyecto"][p] = rec

    # KPIs por Estado (global)
    if "Estado de inmueble" in df_total.columns:
        out["by_estado"] = (
            df_total["Estado de inmueble"].fillna("__NA__").value_counts().to_dict()
        )

    return out

def main():
    registros = []
    frames = []
    base = Path(CARPETA)

    for nombre, fname in ARCHIVOS.items():
        path = base / fname
        if not path.exists():
            print(f"❌ No encontrado: {path}")
            continue
        try:
            df = leer_xls_tolerante(str(path))
            df = estandarizar(df, proyecto=nombre)
            frames.append(df)
            print(f"✅ {nombre}: {len(df)} filas, {len(df.columns)} columnas")
        except Exception as e:
            print(f"⚠️ Error leyendo {nombre}: {e}")

    if not frames:
        print("No se pudo leer ningún archivo.")
        return

    df_total = pd.concat(frames, ignore_index=True)
    # Exportar registros (solo columnas clave + numéricas)
    prefer = [c for c in [
        "Proyecto", "Numero de inmueble", "Precio de lista", "Precio de lista_num",
        "Estado de inmueble", "Tipologia", "Area techada", "Area techada_num",
        "Area total", "Area total_num", "Dormitorios", "Dormitorios_num", "Piso"
    ] if c in df_total.columns]

    records = df_total[prefer].to_dict(orient="records")
    for r in records:
        # Convertir NaN -> None para JSON limpio
        for k, v in list(r.items()):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                r[k] = None

    _export_json(records, SALIDA_RECORDS)
    _export_json(kpis(df_total), SALIDA_KPIS)

    print(f"💾 Exportado {SALIDA_RECORDS} con {len(records)} registros")
    print(f"💾 Exportado {SALIDA_KPIS}")

if __name__ == "__main__":
    main()

