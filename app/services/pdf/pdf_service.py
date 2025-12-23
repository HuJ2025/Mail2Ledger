import os
import io
import requests
import pytesseract
import ocrmypdf
import tempfile
import os, tempfile, subprocess
from pathlib import Path

from PIL import ImageStat

import logging, re
from io import BytesIO
from pdf2image import convert_from_bytes
from pytesseract import Output
from PyPDF2 import PdfReader, PdfWriter

from openai import OpenAI
from dotenv import load_dotenv

from app.utils.common import safe_url, encode_path_minimally

import logging
from typing import Any, Iterable, List, Sequence

load_dotenv()
OPENAI_API = os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key=OPENAI_API)

def is_blank_page(image, threshold=0.98):
    # image: PIL Image
    stat = ImageStat.Stat(image.convert('L'))  # 灰階
    mean = stat.mean[0]
    # 255 完全白，這裡算全白像素的比例
    white_ratio = mean / 255
    return white_ratio > threshold

def upload_pdf2openai(pdf_url: str):
    # Step 1: 下載 PDF
    safe_pdf_url = safe_url(pdf_url)
    #safe_pdf_url = encode_path_minimally(pdf_url)
    response = requests.get(safe_pdf_url)
    if response.status_code != 200:
        raise ValueError(f"❌ Failed to download PDF, status code: {response.status_code}")
    
    original_pdf_bytes = response.content

    # Step 2: 修正 PDF 旋轉
    # corrected_pdf_bytes = correct_pdf_rotation(original_pdf_bytes)
    ocr_pdf_bytes = ocr_from_url(original_pdf_bytes)
    compress_pdf_bytes = compress_if_big(ocr_pdf_bytes)
    # Step 3: 上傳至 OpenAI
    file_stream = io.BytesIO(compress_pdf_bytes)
    file_stream.name = "sample_statement_for_investigation.pdf"  # OpenAI 要求 file-like object 有 name 屬性
    file_response = client.files.create(file=file_stream, purpose='user_data')

    return file_response.id, original_pdf_bytes


def upload_pdf2openai_no_ocr(pdf_url: str):
    # # Step 1: 下載 PDF
    # safe_pdf_url = safe_url(pdf_url)
    # #safe_pdf_url = encode_path_minimally(pdf_url)
    # response = requests.get(safe_pdf_url)
    # if response.status_code != 200:
    #     raise ValueError(f"❌ Failed to download PDF, status code: {response.status_code}")
    # original_pdf_bytes = response.content

    path = Path(pdf_url)
    if not path.is_file():
        raise ValueError(f"❌ PDF file not found: {pdf_url}")
    
    original_pdf_bytes = path.read_bytes()

    # corrected_pdf_bytes = correct_pdf_rotation(pdf_bytes)
    # ocr_pdf_bytes = ocr_from_url(pdf_bytes)
    compress_pdf_bytes = compress_if_big(original_pdf_bytes)
    # Step 3: 上傳至 OpenAI
    file_stream = io.BytesIO(compress_pdf_bytes)
    file_stream.name = "sample_statement_for_investigation.pdf"  # OpenAI 要求 file-like object 有 name 屬性
    file_response = client.files.create(file=file_stream, purpose='user_data')

    return file_response.id, original_pdf_bytes

def ocr_from_url(pdf_bytes):

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as input_file, \
        tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as output_file:

        input_file.write(pdf_bytes)
        input_file.flush()

        ocrmypdf.ocr(
            input_file.name,
            output_file.name,
            #language='eng',
            skip_text=True,
            #force_ocr=True
        )

        with open(output_file.name, 'rb') as f:
            ocr_pdf_bytes = f.read()

    os.unlink(input_file.name)
    os.unlink(output_file.name)

    return ocr_pdf_bytes

def correct_pdf_rotation(pdf_bytes: bytes) -> bytes:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()

    images = convert_from_bytes(pdf_bytes, dpi=300)

    for i, image in enumerate(images):
        # 1. 先判斷是不是空白頁
        if is_blank_page(image, threshold=0.98):
            rotation_angle = 0
        else:
            try:
                osd = pytesseract.image_to_osd(image.convert("RGB"), output_type=Output.DICT)
                rotation_angle = osd.get('rotate', 0)
            except pytesseract.TesseractError:
                rotation_angle = 0  # 或者可以記錄下來 log

        page = reader.pages[i]
        if rotation_angle != 0:
            page.rotate(rotation_angle)
        writer.add_page(page)

    # 將結果寫入 memory
    output_stream = io.BytesIO()
    writer.write(output_stream)
    output_stream.seek(0)
    return output_stream.getvalue()

def delete_openai_file(file_ids):

    if isinstance(file_ids, str):
        file_ids = [file_ids]

    for file_id in file_ids:
        try:
            resp = client.files.delete(file_id)
            print(f"Deleted {file_id}: {resp.deleted}")  # 用 resp.deleted
        except Exception as e:
            print(f"Failed to delete {file_id}: {e}")



MAX_MB = 31            # 阈值：31 MB
GS_PRESET = "/ebook"   # /screen(最小) /ebook /printer /prepress

def compress_if_big(pdf_bytes: bytes) -> bytes:
    """
    如果 pdf_bytes > 31 MB，则用 Ghostscript 压缩后返回；否则原样返回。
    """
    if len(pdf_bytes) <= MAX_MB * 1024 * 1024:
        print(len(pdf_bytes), "bytes, 不超标，直接返回")
        return pdf_bytes      # ★ 不超标，直接返回

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as src, \
            tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as dst:
        src.write(pdf_bytes)
        src.flush()

        # --- 调用 Ghostscript ---
        cmd = [
            "gs", "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.6",
            f"-dPDFSETTINGS={GS_PRESET}",
            "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={dst.name}",
            src.name,
        ]
        subprocess.run(cmd, check=True)

        with open(dst.name, "rb") as f_out:
            compressed = f_out.read()
    print(len(compressed), "bytes, 压缩后")
    # 清理临时文件
    Path(src.name).unlink(missing_ok=True)
    Path(dst.name).unlink(missing_ok=True)

    return compressed

def _split_increasing_digits(s: str, max_value: int | None) -> List[int]:
    """
    Split a digit-only string into a strictly increasing sequence.
    Example: '345678910' -> [3,4,5,6,7,8,9,10]
    Greedy: choose the shortest chunk that is > previous.
    """
    if not s or not s.isdigit():
        return []
    res: List[int] = []
    i, n = 0, len(s)
    max_len = len(str(max_value)) if max_value else 4  # guard; can raise if needed

    while i < n:
        found_len = None
        prev = res[-1] if res else -10**9
        # try 1..max_len digits
        for L in range(1, min(max_len, n - i) + 1):
            num = int(s[i:i + L])
            if num > prev:
                found_len = L
                break
        if found_len is None:
            # last resort: take the rest; if still not > prev, stop parsing
            tail = int(s[i:])
            if tail > prev:
                res.append(tail)
            break
        res.append(int(s[i:i + found_len]))
        i += found_len
    return res

def _flatten(items: Any) -> Iterable[Any]:
    if isinstance(items, (list, tuple, set)):
        for x in items:
            yield from _flatten(x)
    else:
        yield items

def _normalize_pages_input(page_ranges: Any, total_pages: int) -> List[int]:
    """
    Accepts ints/strings/lists, supports:
    - '3-10' ranges, '3,4 5' mixes
    - pure digit tokens -> split to increasing sequence
    - large ints like 345678910 -> split if > total_pages
    Returns sorted unique pages within [1, total_pages].
    """
    out: List[int] = []

    def add_token(tok: str):
        # handle simple ranges inside a token first
        if "-" in tok:
            a, b = tok.split("-", 1)
            if a.isdigit() and b.isdigit():
                start, end = int(a), int(b)
                if start <= end:
                    out.extend(range(start, end + 1))
                else:
                    out.extend(range(end, start + 1))
                return
        # pure digits
        if tok.isdigit():
            if len(tok) > 1:
                out.extend(_split_increasing_digits(tok, max_value=total_pages))
            else:
                out.append(int(tok))

    for item in _flatten(page_ranges):
        if item is None:
            continue
        if isinstance(item, int):
            # if absurdly large (e.g., 345678910), attempt split
            if item <= 0 or item > total_pages:
                out.extend(_split_increasing_digits(str(item), max_value=total_pages))
            else:
                out.append(item)
        elif isinstance(item, str):
            # pull out chunks like 3, 10, 3-10 from messy strings
            for tok in re.findall(r"\d+(?:-\d+)?", item):
                add_token(tok)
        else:
            # last try: cast to int
            try:
                val = int(item)
                if val <= 0 or val > total_pages:
                    out.extend(_split_increasing_digits(str(val), max_value=total_pages))
                else:
                    out.append(val)
            except Exception:
                continue

    # in-bounds, dedup (preserve order), sort increasing
    seen = set()
    cleaned = []
    for p in out:
        if 1 <= p <= total_pages and p not in seen:
            seen.add(p)
            cleaned.append(p)
    return sorted(cleaned)

def slice_pdf(ocr_bytes: bytes, page_ranges: Sequence[Any], prefix: str = "slice",
              include_neighbors: bool = True) -> bytes:
    """
    Robust slicer:
      - Parses messy page specs (concatenated digits, ranges, lists)
      - Ensures strictly increasing pages
      - Optionally includes neighbor pages (p-1, p+1)
      - If nothing valid, returns original PDF to avoid crashes
    """
    if not page_ranges:
        logging.warning("slice_pdf() called with an empty `page_ranges`. Returning original PDF.")
        return ocr_bytes

    reader = PdfReader(BytesIO(ocr_bytes))
    total_pages = len(reader.pages)

    pages = _normalize_pages_input(page_ranges, total_pages)
    print("normalized pages:", pages)
    if not pages:
        logging.warning(f"slice_pdf(): no valid pages after normalization ({page_ranges}). Returning original PDF.")
        return ocr_bytes

    # extend with neighbors (bounded)
    if include_neighbors:
        ext = set(pages)
        for p in pages:
            if p - 1 >= 1: ext.add(p - 1)
            if p + 1 <= total_pages: ext.add(p + 1)
        pages = sorted(ext)

    writer = PdfWriter()
    for p in pages:
        # PdfReader is 0-based; our pages are 1-based
        writer.add_page(reader.pages[p - 1])

    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()

from typing import Any, Dict, List, Tuple

# def extract_page_arrays(data: Dict[str, Any]) -> Tuple[List[int], List[int]]:
#     """
#     Given a dict with keys 'asset_tables' and 'transaction_tables', each a list of
#     items like {'page': 3, 'table_name': '...', 'sub_tables': [...]},
#     return (asset_pages, transaction_pages) as sorted, de-duplicated int lists.

#     - Accepts int/str/float-like page values.
#     - Ignores missing/invalid/<=0 pages.
#     """

#     def _to_int_page(v: Any) -> int | None:
#         if isinstance(v, int):
#             return v if v > 0 else None
#         try:
#             # handles "31", "31.0", 31.0, etc.
#             n = int(float(str(v).strip()))
#             return n if n > 0 else None
#         except Exception:
#             return None

#     def _collect(key: str) -> List[int]:
#         pages = []
#         for item in data.get(key, []) or []:
#             p = _to_int_page(item.get("page"))
#             if p is not None:
#                 pages.append(p)
#         return sorted(set(pages))

#     asset_pages        = _collect("asset_tables")
#     transaction_pages  = _collect("transaction_tables")
#     return asset_pages, transaction_pages


def extract_pages(tables: Iterable[Dict[str, Any]]) -> List[int]:
    """
    Convert a list of table dicts (each having a 'page' field) into a
    sorted, de-duplicated list of positive page numbers.
    Accepts int/str/float-like page values; ignores invalid/missing/<=0.
    """
    pages = set()
    for item in tables or []:
        v = item.get("page") if isinstance(item, dict) else None
        if v is None:
            continue
        try:
            n = int(v) if isinstance(v, int) else int(float(str(v).strip()))
            if n > 0:
                pages.add(n)
        except (ValueError, TypeError):
            continue
    return sorted(pages)