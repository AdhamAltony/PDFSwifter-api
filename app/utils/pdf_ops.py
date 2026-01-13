import os
import asyncio
import inspect
from zipfile import ZipFile

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

try:
    import pdfplumber
except Exception:  # pragma: no cover
    pdfplumber = None


def convert_pdf_tables_to_excel(pdf_path: str, excel_path: str) -> None:
    """Extract tables into an Excel workbook."""
    if pdfplumber is None or pd is None:
        raise RuntimeError(
            "PDF-to-Excel dependencies are missing. Install `pdfplumber` and `pandas` to use this endpoint."
        )

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


def convert_pdf_to_docx(pdf_path: str, word_path: str) -> None:
    """Convert PDF into DOCX using pdf2docx."""
    try:
        from pdf2docx import Converter  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "PDF-to-Word dependency is missing. Install `pdf2docx` to use this endpoint."
        ) from exc

    cv = Converter(pdf_path)
    try:
        cv.convert(word_path, start=0, end=None)
    finally:
        cv.close()


def create_images_zip(pdf_path: str, session_folder: str, zip_path: str, base_name: str) -> None:
    """Render PDF pages to PNG and store them inside a zip archive."""
    if fitz is None:
        raise RuntimeError(
            "PDF-to-Image dependency is missing. Install `PyMuPDF` to use this endpoint."
        )

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


def compress_pdf(input_pdf_path: str, output_pdf_path: str, level: str = "balanced") -> None:
    """Compress a PDF by rewriting it with PyMuPDF save optimizations.

    Notes:
    - This primarily optimizes streams and removes unused objects.
    - Compression effectiveness depends heavily on the PDF contents.
    """

    level = (level or "balanced").strip().lower()
    if level not in {"fast", "balanced", "max"}:
        level = "balanced"

    compression_effort = {"fast": 0, "balanced": 50, "max": 100}[level]
    use_objstms = 1 if level == "max" else 0
    # Newer MuPDF builds may not support linearization; keep it off for compatibility.
    linear = 0

    if fitz is None:
        raise RuntimeError(
            "PDF compression dependency is missing. Install `PyMuPDF` to use this endpoint."
        )

    with fitz.open(input_pdf_path) as doc:
        save_kwargs = {
            "garbage": 4,
            "clean": 1,
            "deflate": 1,
            "deflate_images": 1,
            "deflate_fonts": 1,
            "incremental": 0,
            "linear": linear,
            "use_objstms": use_objstms,
            "compression_effort": compression_effort,
        }

        # Filter options to match the installed PyMuPDF version.
        params = inspect.signature(fitz.Document.save).parameters
        filtered = {k: v for k, v in save_kwargs.items() if k in params}

        doc.save(output_pdf_path, **filtered)
