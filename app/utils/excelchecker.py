# app/utils/excelchecker.py
import json, os, tempfile
from io import BytesIO
from uuid import uuid4

import psycopg2.extras as pg_extras
import msoffcrypto

OLE2_SIG = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"


def ensure_openpyxl_readable_xlsx(file_path: str, password: str | None) -> tuple[str, bool]:
    """
    Returns: (path_to_real_xlsx_zip, created_temp)
    - If input is normal xlsx (PK..), return original path.
    - If input is encrypted OLE2 (D0 CF..), decrypt to a temp .xlsx and return temp path.
    """
    with open(file_path, "rb") as f:
        head8 = f.read(8)

    # normal xlsx -> zip -> PK
    if head8[:2] == b"PK":
        return file_path, False

    # encrypted OOXML stored in OLE2 container
    if head8 == OLE2_SIG:
        if not password:
            raise RuntimeError("Excel is encrypted. Please provide password.")

        with open(file_path, "rb") as f:
            office = msoffcrypto.OfficeFile(f)
            if not office.is_encrypted():
                raise RuntimeError("OLE2 file but not recognized as encrypted workbook (unsupported).")

            office.load_key(password=password)
            buf = BytesIO()
            office.decrypt(buf)
            data = buf.getvalue()

        if not data.startswith(b"PK"):
            raise RuntimeError("Decryption output is not a valid .xlsx(zip). Password may be wrong.")

        tmp_path = os.path.join(
            tempfile.gettempdir(),
            f"decrypted_{os.path.basename(file_path)}_{uuid4().hex}.xlsx",
        )
        with open(tmp_path, "wb") as wf:
            wf.write(data)

        return tmp_path, True

    raise RuntimeError("Unsupported file format (not xlsx zip, not encrypted OLE2).")


def ensure_openpyxl_readable_xlsx_bytes(data: bytes, password: str | None) -> bytes:
    """
    In-memory version.
    - If normal xlsx zip (PK..): return original bytes
    - If encrypted OLE2 (D0 CF..): decrypt in memory and return xlsx zip bytes
    """
    if not data or len(data) < 8:
        raise RuntimeError("Empty/invalid excel bytes")

    head8 = data[:8]

    # normal xlsx -> zip -> PK
    if head8[:2] == b"PK":
        return data

    # encrypted OOXML stored in OLE2 container
    if head8 == OLE2_SIG:
        if not password:
            raise RuntimeError("Excel is encrypted. Please provide password.")

        office = msoffcrypto.OfficeFile(BytesIO(data))
        if not office.is_encrypted():
            raise RuntimeError("OLE2 file but not recognized as encrypted workbook (unsupported).")

        office.load_key(password=password)
        buf = BytesIO()
        office.decrypt(buf)
        out = buf.getvalue()

        if not out.startswith(b"PK"):
            raise RuntimeError("Decryption output is not a valid .xlsx(zip). Password may be wrong.")
        return out

    raise RuntimeError("Unsupported file format (not xlsx zip, not encrypted OLE2).")