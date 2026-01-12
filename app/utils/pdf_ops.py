import os
import asyncio
from zipfile import ZipFile

import fitz
import pandas as pd
import pdfplumber

from pdf2docx import Converter


def convert_pdf_tables_to_excel(pdf_path: str, excel_path: str) -> None:
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


def convert_pdf_to_docx(pdf_path: str, word_path: str) -> None:
    """Convert PDF into DOCX using pdf2docx."""
    cv = Converter(pdf_path)
    try:
        cv.convert(word_path, start=0, end=None)
    finally:
        cv.close()


def create_images_zip(pdf_path: str, session_folder: str, zip_path: str, base_name: str) -> None:
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
