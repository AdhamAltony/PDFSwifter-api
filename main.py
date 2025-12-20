import asyncio
import os
import re
import shutil
import threading
import time
import unicodedata
import uuid
from zipfile import ZipFile

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
import yt_dlp
import pdfplumber
import pandas as pd
import fitz

from pdf2docx import Converter

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
WORD_DOWNLOAD_FOLDER = "word_outputs"
os.makedirs(WORD_DOWNLOAD_FOLDER, exist_ok=True)
IMAGE_DOWNLOAD_FOLDER = "image_outputs"
os.makedirs(IMAGE_DOWNLOAD_FOLDER, exist_ok=True)

CHUNK_SIZE = 1024 * 1024  # 1MB



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


def safe_stem(filename):
    """Return a sanitized stem for derived files."""
    stem = os.path.splitext(os.path.basename(filename))[0]
    sanitized = ascii_filename(stem)
    return sanitized or "file"


async def save_upload_file(upload_file: UploadFile, destination_folder: str) -> str:
    """Persist UploadFile contents to disk using chunks to avoid large memory spikes."""
    ext = os.path.splitext(upload_file.filename)[1]
    unique_name = uuid.uuid4().hex + (ext.lower() if ext else "")
    file_path = os.path.join(destination_folder, unique_name)

    with open(file_path, "wb") as buffer:
        while True:
            chunk = await upload_file.read(CHUNK_SIZE)
            if not chunk:
                break
            buffer.write(chunk)

    await upload_file.seek(0)
    return file_path


def convert_pdf_tables_to_excel(pdf_path: str, excel_path: str):
    """Extract tables into an Excel workbook."""
    all_tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                df = pd.DataFrame(table[1:], columns=table[0])
                all_tables.append(df)

    if not all_tables:
        raise ValueError("No tables found in PDF.")

    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        for i, df in enumerate(all_tables):
            sheet_name = f"Sheet{i+1}"
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def convert_pdf_to_docx(pdf_path: str, word_path: str):
    """Convert PDF into DOCX using pdf2docx."""
    cv = Converter(pdf_path)
    try:
        cv.convert(word_path, start=0, end=None)
    finally:
        cv.close()


def create_images_zip(pdf_path: str, session_folder: str, zip_path: str, base_name: str):
    """Render PDF pages to PNG and store them inside a zip archive."""
    image_paths = []
    with fitz.open(pdf_path) as doc:
        if doc.page_count == 0:
            raise ValueError("No pages found in PDF.")

        for page_index in range(doc.page_count):
            page = doc.load_page(page_index)
            pix = page.get_pixmap()
            image_path = os.path.join(
                session_folder, f"{base_name}_page_{page_index + 1}.png"
            )
            pix.save(image_path)
            image_paths.append(image_path)

    with ZipFile(zip_path, "w") as zip_file:
        for image_path in image_paths:
            zip_file.write(image_path, arcname=os.path.basename(image_path))


def download_video(url: str, output_template: str, custom_options=None) -> str:
    """Download remote video content to disk and return the resulting filename."""
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
        "outtmpl": output_template,
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": True,
    }

    if custom_options:
        ydl_opts.update(custom_options)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url)
        filename = ydl.prepare_filename(info)
        if not filename.lower().endswith(".mp4"):
            filename = os.path.splitext(filename)[0] + ".mp4"

    if not os.path.exists(filename):
        raise FileNotFoundError("Failed to download video.")

    return filename


# =============================
# Endpoint youtube downloader
# =============================
@app.get("/youtube/download")
async def download_youtube(url: str):
    """
    Download a YouTube video as MP4 (video + audio merged)
    and return it as a file response.
    """
    output_template = os.path.join(DOWNLOAD_FOLDER, "%(id)s_%(title)s.%(ext)s")

    try:
        filename = await asyncio.to_thread(download_video, url, output_template)
    except Exception as e:
        return {"error": f"Failed to download video: {str(e)}"}

    delete_file_later(filename)
    safe_filename = ascii_filename(os.path.basename(filename))
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(filename, filename=safe_filename, headers=headers)

# =============================
# Endpoint TikTok Download
# =============================
@app.get("/tiktok/download")
async def download_tiktok(url: str):
    """
    Download a TikTok video as MP4 to support high-volume workloads.
    """
    output_template = os.path.join(
        DOWNLOAD_FOLDER, "tiktok_%(id)s_%(upload_date)s_%(timestamp)s.%(ext)s"
    )

    # TikTok URLs are handled by yt_dlp; tweak retries for resiliency.
    custom_options = {
        "retries": 5,
        "fragment_retries": 5,
        "skip_unavailable_fragments": True,
    }

    try:
        filename = await asyncio.to_thread(
            download_video, url, output_template, custom_options
        )
    except Exception as e:
        return {"error": f"Failed to download TikTok video: {str(e)}"}

    delete_file_later(filename)
    safe_filename = ascii_filename(os.path.basename(filename))
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(filename, filename=safe_filename, headers=headers)

# =============================
# Endpoint PDF To Excel
# =============================
@app.post("/pdf/to-excel")
async def pdf_to_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        return {"error": "Please upload a PDF file."}

    pdf_path = await save_upload_file(file, PDF_DOWNLOAD_FOLDER)

    base_name = safe_stem(file.filename)
    unique_id = uuid.uuid4().hex
    excel_filename = f"{base_name}_{unique_id}.xlsx"
    excel_path = os.path.join(EXCEL_DOWNLOAD_FOLDER, excel_filename)

    try:
        await asyncio.to_thread(convert_pdf_tables_to_excel, pdf_path, excel_path)
    except ValueError as e:
        if os.path.exists(excel_path):
            os.remove(excel_path)
        delete_file_later(pdf_path)
        return {"error": str(e)}
    except Exception as e:
        if os.path.exists(excel_path):
            os.remove(excel_path)
        delete_file_later(pdf_path)
        return {"error": f"Failed to convert PDF: {str(e)}"}

    delete_file_later(pdf_path)
    delete_file_later(excel_path, delay=600)

    safe_filename = ascii_filename(os.path.basename(excel_path))
    headers = {"Content-Disposition": f'attachment; filename=\"{safe_filename}\"'}
    return FileResponse(excel_path, filename=safe_filename, headers=headers)

# =============================
# Endpoint PDF To Word
# =============================
@app.post("/pdf/to-word")
async def pdf_to_word(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        return {"error": "Please upload a PDF file."}

    pdf_path = await save_upload_file(file, PDF_DOWNLOAD_FOLDER)

    base_name = safe_stem(file.filename)
    unique_id = uuid.uuid4().hex
    word_filename = f"{base_name}_{unique_id}.docx"
    word_path = os.path.join(WORD_DOWNLOAD_FOLDER, word_filename)

    try:
        await asyncio.to_thread(convert_pdf_to_docx, pdf_path, word_path)
    except Exception as e:
        if os.path.exists(word_path):
            os.remove(word_path)
        delete_file_later(pdf_path)
        return {"error": f"Failed to convert PDF: {str(e)}"}

    delete_file_later(pdf_path)
    delete_file_later(word_path, delay=600)

    safe_filename = ascii_filename(os.path.basename(word_path))
    headers = {"Content-Disposition": f'attachment; filename=\"{safe_filename}\"'}
    return FileResponse(word_path, filename=safe_filename, headers=headers)

# =============================
# Endpoint PDF To Image
# =============================
@app.post("/pdf/to-image")
async def pdf_to_image(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        return {"error": "Please upload a PDF file."}

    pdf_path = await save_upload_file(file, PDF_DOWNLOAD_FOLDER)

    base_name = safe_stem(file.filename)
    unique_id = uuid.uuid4().hex
    session_folder = os.path.join(IMAGE_DOWNLOAD_FOLDER, f"{base_name}_{unique_id}")
    os.makedirs(session_folder, exist_ok=True)
    zip_path = os.path.join(IMAGE_DOWNLOAD_FOLDER, f"{base_name}_{unique_id}.zip")

    try:
        await asyncio.to_thread(
            create_images_zip, pdf_path, session_folder, zip_path, base_name
        )
    except ValueError as e:
        shutil.rmtree(session_folder, ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        delete_file_later(pdf_path)
        return {"error": str(e)}
    except Exception as e:
        shutil.rmtree(session_folder, ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        delete_file_later(pdf_path)
        return {"error": f"Failed to convert PDF: {str(e)}"}

    shutil.rmtree(session_folder, ignore_errors=True)
    delete_file_later(pdf_path)
    delete_file_later(zip_path, delay=600)

    safe_filename = ascii_filename(os.path.basename(zip_path))
    headers = {"Content-Disposition": f'attachment; filename=\"{safe_filename}\"'}
    return FileResponse(zip_path, filename=safe_filename, headers=headers)
