import pandas as pd

def generar_reporte(adjudicadas_path: str, ofertas_path: str, output_path: str):
    df_adj = pd.read_excel(adjudicadas_path)
    df_oft = pd.read_excel(ofertas_path)

    df_adj.columns = [str(c).strip() for c in df_adj.columns]
    df_oft.columns = [str(c).strip() for c in df_oft.columns]

    salida = pd.DataFrame({
        "Mensaje": ["La carga de ambos archivos funciona correctamente"]
    })

    salida.to_excel(output_path, index=False)

    return {
        "filas_adjudicadas": int(len(df_adj)),
        "filas_ofertas": int(len(df_oft)),
        "sin_criterio": 0,
        "correcciones": []
    }
