import os
import re
import threading
import time
import unicodedata

from fastapi import FastAPI
from fastapi.responses import FileResponse
import yt_dlp

from fastapi import UploadFile, File
import pdfplumber
import pandas as pd


# =============================
# App and Constants
# =============================
app = FastAPI()
DOWNLOAD_FOLDER = "downloads"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
PDF_DOWNLOAD_FOLDER = "pdf_uploads"
EXCEL_DOWNLOAD_FOLDER = "excel_outputs"
os.makedirs(PDF_DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(EXCEL_DOWNLOAD_FOLDER, exist_ok=True)

# =============================
# Utility Functions
# =============================
def ascii_filename(filename):
    """Sanitize filename for HTTP headers (ASCII only)."""
    nfkd = unicodedata.normalize("NFKD", filename)
    only_ascii = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"[^A-Za-z0-9._-]", "_", only_ascii)


def delete_file_later(file_path, delay=300):
    """Delete a file after a delay (default 5 minutes)."""
    def delete():
        time.sleep(delay)
        if os.path.exists(file_path):
            os.remove(file_path)

    threading.Thread(target=delete, daemon=True).start()


# =============================
# Endpoint
# =============================
@app.get("/youtube/download")
def download_youtube(url):
    """
    Download a YouTube video as MP4 (video + audio merged)
    and return it as a file response.
    """
    output_template = os.path.join(DOWNLOAD_FOLDER, "%(title)s.%(ext)s")
    
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
        "outtmpl": output_template,
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url)
            filename = ydl.prepare_filename(info)
            # yt_dlp may add the extension if merging
            if not filename.lower().endswith(".mp4"):
                filename = os.path.splitext(filename)[0] + ".mp4"

            if not os.path.exists(filename):
                return {"error": "Failed to download video."}

    except Exception as e:
        return {"error": f"Failed to download video: {str(e)}"}

    delete_file_later(filename)
    safe_filename = ascii_filename(os.path.basename(filename))
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(filename, filename=safe_filename, headers=headers)

# =============================
# Endpoint
# =============================
@app.post("/pdf/to-excel")
async def pdf_to_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        return {"error": "Please upload a PDF file."}

    pdf_path = os.path.join(PDF_DOWNLOAD_FOLDER, file.filename)
    with open(pdf_path, "wb") as f:
        f.write(await file.read())

    base_name = os.path.splitext(os.path.basename(file.filename))[0]
    excel_path = os.path.join(EXCEL_DOWNLOAD_FOLDER, base_name + ".xlsx")

    try:
        all_tables = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    all_tables.append(df)

        if not all_tables:
            return {"error": "No tables found in PDF."}

        with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
            for i, df in enumerate(all_tables):
                sheet_name = f"Sheet{i+1}"
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    except Exception as e:
        return {"error": f"Failed to convert PDF: {str(e)}"}

    delete_file_later(pdf_path)
    delete_file_later(excel_path, delay=600)

    safe_filename = ascii_filename(os.path.basename(excel_path))
    headers = {"Content-Disposition": f'attachment; filename=\"{safe_filename}\"'}
    return FileResponse(excel_path, filename=safe_filename, headers=headers)
