import pandas as pd
import numpy as np


def _clean_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _to_number(series):
    return pd.to_numeric(series, errors="coerce")


def _manual_invited(value):
    v = _clean_text(value).lower()
    return v in {"yes", "y", "si", "sí", "s"}


def _convert_unit_price(price, from_unit, to_unit):
    from_unit = _clean_text(from_unit).lower()
    to_unit = _clean_text(to_unit).lower()

    if pd.isna(price):
        return np.nan

    if from_unit == to_unit:
        return price

    if from_unit == "kilos" and to_unit == "toneladas":
        return price * 1000

    if from_unit == "toneladas" and to_unit == "kilos":
        return price / 1000

    return price


def _normalize_adjudicadas_columns(df):
    df.columns = [str(c).strip() for c in df.columns]

rename_map = {
    "Unidad de Medica": "Unidad de Medida",
    "Unidad de Medida ": "Unidad de Medida",
    "Unidad de medida": "Unidad de Medida",
    "unidad de medida": "Unidad de Medida",
    "unidad de medica": "Unidad de Medida",
    "Cantidad Adutidaca": "Cantidad Adjudicada",
    "Cantidad Adjudicada ": "Cantidad Adjudicada",
    "cantidad adjudicada": "Cantidad Adjudicada",
    "Precio moneda local": "Precio en Moneda Local",
    "precio moneda local": "Precio en Moneda Local",
    "Precio UNitario": "Precio Unitario",
    "precio unitario": "Precio Unitario",
    "Fecha Adjudicación /Deserción": "Fecha Adjudicación",
    "fecha adjudicación": "Fecha Adjudicación",
}

    return df.rename(columns=rename_map)


def _normalize_ofertas_columns(df):
    df.columns = [str(c).strip() for c in df.columns]

    rename_map = {
        "Nro Licitación": "Nro. Licitación",
    }

    return df.rename(columns=rename_map)


def generar_reporte(adjudicadas_path: str, ofertas_path: str, output_path: str):
    df_adj = pd.read_excel(adjudicadas_path)
    df_oft = pd.read_excel(ofertas_path)

    df_adj = _normalize_adjudicadas_columns(df_adj)
    df_oft = _normalize_ofertas_columns(df_oft)

    required_adj = [
        "Nro. Licitación",
        "SKU",
        "Producto",
        "Unidad de Medida",
        "Fecha Adjudicación",
        "Cantidad Adjudicada",
        "Precio en Moneda Local",
        "Monto",
    ]

    required_oft = [
        "Nro. Licitación",
        "SKU",
        "Invitado Manual",
        "Precio en Moneda Local",
    ]

    faltantes_adj = [c for c in required_adj if c not in df_adj.columns]
    faltantes_oft = [c for c in required_oft if c not in df_oft.columns]

    if faltantes_adj:
        raise ValueError(f"Faltan columnas en Adjudicadas: {faltantes_adj}")

    if faltantes_oft:
        raise ValueError(f"Faltan columnas en Ofertas: {faltantes_oft}")

    # Normalización de tipos
    df_adj["Nro. Licitación"] = df_adj["Nro. Licitación"].astype(str).str.strip()
    df_adj["SKU"] = df_adj["SKU"].astype(str).str.strip()
    df_adj["Producto"] = df_adj["Producto"].astype(str).str.strip()
    df_adj["Unidad de Medida"] = df_adj["Unidad de Medida"].astype(str).str.strip()
    df_adj["Fecha Adjudicación"] = pd.to_datetime(df_adj["Fecha Adjudicación"], errors="coerce")
    df_adj["Cantidad Adjudicada"] = _to_number(df_adj["Cantidad Adjudicada"])
    df_adj["Precio en Moneda Local"] = _to_number(df_adj["Precio en Moneda Local"])
    df_adj["Monto"] = _to_number(df_adj["Monto"])

    df_oft["Nro. Licitación"] = df_oft["Nro. Licitación"].astype(str).str.strip()
    df_oft["SKU"] = df_oft["SKU"].astype(str).str.strip()
    df_oft["Precio en Moneda Local"] = _to_number(df_oft["Precio en Moneda Local"])
    df_oft["Invitado Manual Flag"] = df_oft["Invitado Manual"].apply(_manual_invited)

    # Clave compuesta
    df_adj["clave"] = df_adj["SKU"] + "_" + df_adj["Nro. Licitación"]
    df_oft["clave"] = df_oft["SKU"] + "_" + df_oft["Nro. Licitación"]

    # Filtro 2026
    df_2026 = df_adj[df_adj["Fecha Adjudicación"].dt.year == 2026].copy()

    correcciones = []
    sin_criterio = 0
    filas_sin_ultima = 0
    filas_sin_manual = 0
    filas_sin_media_ofertas = 0

    output_rows = []

    # Historial por SKU para facilitar cálculos
    grouped_by_sku = {sku: g.copy() for sku, g in df_adj.groupby("SKU")}

    for _, row in df_2026.iterrows():
        lic = row["Nro. Licitación"]
        sku = row["SKU"]
        producto = row["Producto"]
        unidad_actual = row["Unidad de Medida"]
        fecha_adj = row["Fecha Adjudicación"]
        cantidad_adj = row["Cantidad Adjudicada"]
        monto_total = row["Monto"]
        precio_adj = row["Precio en Moneda Local"]
        clave = row["clave"]

        ultima_compra = np.nan
        fecha_ultima_compra = pd.NaT
        media_inv_manual = np.nan
        media_ofertas = np.nan
        criterio_aplicado = ""
        precio_referencia_final = np.nan
        ahorro = 0.0

        # -------------------------
        # 1) ÚLTIMA COMPRA
        # -------------------------
        hist_sku = grouped_by_sku.get(sku, pd.DataFrame()).copy()

        if not hist_sku.empty:
            hist_prev = hist_sku[hist_sku["Nro. Licitación"] != lic].copy()

            if not hist_prev.empty:
                fecha_max = hist_prev["Fecha Adjudicación"].max()

                if pd.notna(fecha_max):
                    grupo_ultima = hist_prev[hist_prev["Fecha Adjudicación"] == fecha_max].copy()

                    if not grupo_ultima.empty:
                        precios_hist_sku = hist_sku["Precio en Moneda Local"].dropna()
                        mediana_hist = precios_hist_sku.median() if not precios_hist_sku.empty else np.nan

                        precios_convertidos = []

                        for _, r2 in grupo_ultima.iterrows():
                            precio_ref = r2["Precio en Moneda Local"]
                            unidad_ref = r2["Unidad de Medida"]

                            if pd.notna(mediana_hist) and pd.notna(precio_ref):
                                if precio_ref < (0.05 * mediana_hist):
                                    precio_ref = precio_ref * 1000
                                    correcciones.append({
                                        "SKU": sku,
                                        "Nro. Licitación": r2["Nro. Licitación"],
                                        "Precio original": float(r2["Precio en Moneda Local"]) if pd.notna(r2["Precio en Moneda Local"]) else None,
                                        "Precio corregido": float(precio_ref),
                                    })

                            precio_ref = _convert_unit_price(precio_ref, unidad_ref, unidad_actual)
                            precios_convertidos.append(precio_ref)

                        precios_convertidos = [p for p in precios_convertidos if pd.notna(p)]

                        if precios_convertidos:
                            ultima_compra = float(np.mean(precios_convertidos))
                            fecha_ultima_compra = fecha_max

        if pd.isna(ultima_compra):
            filas_sin_ultima += 1

        # -------------------------
        # 2) MEDIA INVITADOS MANUALES
        # -------------------------
        ofertas_clave = df_oft[df_oft["clave"] == clave].copy()

        if not ofertas_clave.empty:
            ofertas_manual = ofertas_clave[ofertas_clave["Invitado Manual Flag"] == True].copy()
            ofertas_manual = ofertas_manual[pd.notna(ofertas_manual["Precio en Moneda Local"])]

            if not ofertas_manual.empty:
                media_inv_manual = float(ofertas_manual["Precio en Moneda Local"].mean())

        if pd.isna(media_inv_manual):
            filas_sin_manual += 1

        # -------------------------
        # 3) MEDIA DE OFERTAS
        # -------------------------
        if not ofertas_clave.empty and pd.notna(precio_adj):
            ofertas_validas = ofertas_clave[pd.notna(ofertas_clave["Precio en Moneda Local"])].copy()
            ofertas_validas = ofertas_validas[
                ofertas_validas["Precio en Moneda Local"] <= (1.5 * precio_adj)
            ]

            if not ofertas_validas.empty:
                media_ofertas = float(ofertas_validas["Precio en Moneda Local"].mean())

        if pd.isna(media_ofertas):
            filas_sin_media_ofertas += 1

        # -------------------------
        # PRECIO DE REFERENCIA FINAL
        # -------------------------
        if pd.notna(ultima_compra):
            precio_referencia_final = ultima_compra
            criterio_aplicado = "Última Compra"
        elif pd.notna(media_inv_manual):
            precio_referencia_final = media_inv_manual
            criterio_aplicado = "Media Invitados Manuales"
        elif pd.notna(media_ofertas):
            precio_referencia_final = media_ofertas
            criterio_aplicado = "Media de Ofertas"
        else:
            sin_criterio += 1

        # -------------------------
        # AHORRO
        # -------------------------
        if pd.notna(precio_referencia_final) and pd.notna(precio_adj):
            if precio_referencia_final > precio_adj:
                ahorro = float(precio_referencia_final - precio_adj)
            else:
                ahorro = 0.0
        else:
            ahorro = 0.0

        output_rows.append({
            "Nro. Licitación": lic,
            "SKU": sku,
            "Producto": producto,
            "Unidad de medida": unidad_actual,
            "Fecha Adjudicación": fecha_adj,
            "Cantidad Adjudicada": cantidad_adj,
            "Monto Total Adjudicado": monto_total,
            "Precio Unitario Adjudicado": precio_adj,
            "Última Compra": ultima_compra,
            "Fecha Última Compra": fecha_ultima_compra,
            "Media Inv. Manuales": media_inv_manual,
            "Media Ofertas": media_ofertas,
            "Precio Referencia Final": precio_referencia_final,
            "Criterio Aplicado": criterio_aplicado,
            "Ahorro": ahorro,
        })

    df_out = pd.DataFrame(output_rows)

    # Formato básico de fechas
    if "Fecha Adjudicación" in df_out.columns:
        df_out["Fecha Adjudicación"] = pd.to_datetime(df_out["Fecha Adjudicación"], errors="coerce")
    if "Fecha Última Compra" in df_out.columns:
        df_out["Fecha Última Compra"] = pd.to_datetime(df_out["Fecha Última Compra"], errors="coerce")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, sheet_name="Reporte Ahorro 2026", index=False)

    return {
        "filas_2026": int(len(df_2026)),
        "sin_criterio": int(sin_criterio),
        "sin_ultima_compra": int(filas_sin_ultima),
        "sin_media_invitados_manuales": int(filas_sin_manual),
        "sin_media_ofertas": int(filas_sin_media_ofertas),
        "correcciones_detectadas": correcciones[:50],
    }
