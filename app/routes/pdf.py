import os
import asyncio
import shutil
import uuid

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

from app.config import (
    DOWNLOAD_FOLDER,
    EXCEL_DOWNLOAD_FOLDER,
    IMAGE_DOWNLOAD_FOLDER,
    PDF_DOWNLOAD_FOLDER,
    DOWNLOAD_RETENTION_SECONDS,
    UPLOAD_RETENTION_SECONDS,
    WORD_DOWNLOAD_FOLDER,
)

from app.services.download_tracker import DOWNLOAD_TRACKER

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
    compress_pdf,
)

router = APIRouter(prefix="/pdf", tags=["PDF"])

PDF_COMPRESS_CONCURRENCY = int(os.environ.get("PDF_COMPRESS_CONCURRENCY", "4"))
_PDF_COMPRESS_SEMAPHORE = asyncio.Semaphore(max(PDF_COMPRESS_CONCURRENCY, 1))


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
        delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
        return {"error": str(e)}
    except Exception as e:
        if os.path.exists(excel_path):
            os.remove(excel_path)
        delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
        return {"error": f"Failed to convert PDF: {str(e)}"}

    delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
    delete_file_later(excel_path, delay=DOWNLOAD_RETENTION_SECONDS)

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
        delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
        return {"error": f"Failed to convert PDF: {str(e)}"}

    delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
    delete_file_later(word_path, delay=DOWNLOAD_RETENTION_SECONDS)

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
        delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
        return {"error": str(e)}
    except Exception as e:
        shutil.rmtree(session_folder, ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
        return {"error": f"Failed to convert PDF: {str(e)}"}

    shutil.rmtree(session_folder, ignore_errors=True)
    delete_file_later(pdf_path, delay=UPLOAD_RETENTION_SECONDS)
    delete_file_later(zip_path, delay=DOWNLOAD_RETENTION_SECONDS)

    safe_filename = ascii_filename(os.path.basename(zip_path))
    headers = {"Content-Disposition": f'attachment; filename="{safe_filename}"'}
    return FileResponse(zip_path, filename=safe_filename, headers=headers)


@router.post("/compress")
async def compress_pdf_endpoint(file: UploadFile = File(...), level: str = "balanced"):
    """Compress a PDF and return a job id.

    Poll status: GET /downloads/{process_id}
    Download result: GET /downloads/{process_id}/file
    """

    if not file.filename.lower().endswith(".pdf"):
        return {"error": "Please upload a PDF file."}

    input_pdf_path = await save_upload_file(file, PDF_DOWNLOAD_FOLDER)

    base_name = safe_stem(file.filename)
    unique_id = uuid.uuid4().hex
    suggested_name = f"{base_name}_{unique_id}_compressed.pdf"
    output_pdf_path = os.path.join(DOWNLOAD_FOLDER, suggested_name)

    job = DOWNLOAD_TRACKER.create_job(source="pdf_compress", url=file.filename)

    async def runner():
        DOWNLOAD_TRACKER.update_job(job.process_id, status="running", progress=0.0)

        try:
            async with _PDF_COMPRESS_SEMAPHORE:
                # Ensure output directory exists
                os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)
                await asyncio.to_thread(compress_pdf, input_pdf_path, output_pdf_path, level)
                
                # Verify output was actually created and is valid
                if not os.path.exists(output_pdf_path):
                    raise RuntimeError("Compression failed: output file was not created")
                if os.path.getsize(output_pdf_path) < 100:
                    raise RuntimeError(f"Compression failed: output file is only {os.path.getsize(output_pdf_path)} bytes")
        except Exception as exc:
            if os.path.exists(output_pdf_path):
                try:
                    os.remove(output_pdf_path)
                except Exception:
                    pass
            message = str(exc).replace("\n", " ").strip()
            DOWNLOAD_TRACKER.update_job(job.process_id, status="failed", error=message)
            delete_file_later(input_pdf_path, delay=UPLOAD_RETENTION_SECONDS)
            return

        DOWNLOAD_TRACKER.update_job(
            job.process_id,
            status="completed",
            progress=100.0,
            file_path=output_pdf_path,
            suggested_name=suggested_name,
        )

        delete_file_later(input_pdf_path, delay=UPLOAD_RETENTION_SECONDS)
        delete_file_later(output_pdf_path, delay=DOWNLOAD_RETENTION_SECONDS)

    asyncio.create_task(runner())
    return {"process_id": job.process_id}
