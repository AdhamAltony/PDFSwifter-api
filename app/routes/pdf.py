import os
import asyncio
import shutil
import uuid

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

from app.config import (
    EXCEL_DOWNLOAD_FOLDER,
    IMAGE_DOWNLOAD_FOLDER,
    PDF_DOWNLOAD_FOLDER,
    WORD_DOWNLOAD_FOLDER,
)

from app.utils.file_ops import (
    ascii_filename,
    delete_file_later,
    safe_stem,
    save_upload_file,
)
from app.utils.pdf_ops import (
    convert_pdf_tables_to_excel,
    convert_pdf_to_docx,
    create_images_zip,
)

router = APIRouter(prefix="/pdf", tags=["PDF"])


@router.post("/to-excel")
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
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(excel_path, filename=safe_filename, headers=headers)


@router.post("/to-word")
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
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(word_path, filename=safe_filename, headers=headers)


@router.post("/to-image")
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
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(zip_path, filename=safe_filename, headers=headers)
