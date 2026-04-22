from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import os
import uuid
import shutil

from app.processor import generar_reporte

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

UPLOAD_DIR = "/tmp/uploads"
OUTPUT_DIR = "/tmp/outputs"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/procesar")
async def procesar(
    adjudicadas_file: UploadFile = File(...),
    ofertas_file: UploadFile = File(...)
):
    if not adjudicadas_file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="El archivo de adjudicadas debe ser .xlsx")

    if not ofertas_file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="El archivo de ofertas debe ser .xlsx")

    adjudicadas_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{adjudicadas_file.filename}")
    ofertas_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{ofertas_file.filename}")
    output_name = f"Reporte_Ahorro_2026_{uuid.uuid4().hex[:8]}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, output_name)

    with open(adjudicadas_path, "wb") as f:
        shutil.copyfileobj(adjudicadas_file.file, f)

    with open(ofertas_path, "wb") as f:
        shutil.copyfileobj(ofertas_file.file, f)

    try:
        resumen = generar_reporte(adjudicadas_path, ofertas_path, output_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando archivos: {str(e)}")

    return JSONResponse({
        "ok": True,
        "download_url": f"/descargar/{output_name}",
        "resumen": resumen
    })

@app.get("/descargar/{filename}")
def descargar(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )
