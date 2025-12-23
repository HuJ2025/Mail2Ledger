from app.prompts.header_template_prompts import (
  UBS_asset_header_template,
  SC_asset_header_template,
  JP_asset_header_template,
  BOS_asset_header_template,
  LGT_asset_header_template,
  UOB_asset_header_template,
  JSS_asset_header_template,
  UBP_asset_header_template,
  CAI_asset_header_template,
  VPB_asset_header_template,
  MSP_asset_header_template,
  UKH_asset_header_template,
)
from app.prompts.header_template_prompts import (
  UBS_trans_header_template,
  SC_trans_header_template,
  JP_trans_header_template,
  BOS_trans_header_template,
  LGT_trans_header_template,
  UOB_trans_header_template,
  JSS_trans_header_template,
  UBP_trans_header_template,
)
MAPPING_SC_ASSETS = SC_asset_header_template
MAPPING_SC_TXN = SC_trans_header_template
# JPMorgan
MAPPING_JPM_ASSETS = JP_asset_header_template
MAPPING_JPM_TXN = JP_trans_header_template
# ------------------------------ UBS assets ------------------------------ #
MAPPING_UBS_ASSETS = UBS_asset_header_template
MAPPING_UBS_TXN = UBS_trans_header_template
# BOS – Assets
MAPPING_BOS_ASSETS = BOS_asset_header_template
MAPPING_BOS_TXN = BOS_trans_header_template
# LGT assets / txn
MAPPING_LGT_ASSETS = LGT_asset_header_template
MAPPING_LGT_TXN = LGT_trans_header_template
# UOB assets / txn
MAPPING_UOB_ASSETS = UOB_asset_header_template
MAPPING_UOB_TXN = UOB_trans_header_template
# J. Safra Sarasin assets (transactions unavailable in original prompt)
MAPPING_JSS_ASSETS = JSS_asset_header_template
MAPPING_JSS_TXN = JSS_trans_header_template   # (transactions prompt not provided)

# UBP assets / txn
MAPPING_UBP_ASSETS = UBP_asset_header_template
MAPPING_UBP_TXN = UBP_trans_header_template   # (transaction mapping placeholder)

MAPPING_CAI_ASSETS = CAI_asset_header_template
MAPPING_CAI_TXN = []

# VPB – Assets / txn
MAPPING_VPB_ASSETS = VPB_asset_header_template
MAPPING_VPB_TXN = []

# MSP – Assets / txn
MAPPING_MSP_ASSETS = MSP_asset_header_template
MAPPING_MSP_TXN = []

# UKH – Assets / txn
MAPPING_UKH_ASSETS = UKH_asset_header_template
MAPPING_UKH_TXN = []

# Generic "Other"
MAPPING_OTHER_ASSETS = []   # would use DATA_EXTRACTOR_OTHER logic at run‑time
MAPPING_OTHER_TXN   = []

# ──────────────────────────────────────────────────────────────
# Combine into one big dict
# ──────────────────────────────────────────────────────────────
BANK_MAPPINGS = {
    "scb":            {"assets": MAPPING_SC_ASSETS, "transactions": MAPPING_SC_TXN},
    "jpm":           {"assets": MAPPING_JPM_ASSETS,"transactions": MAPPING_JPM_TXN},
    "ubs":                {"assets": MAPPING_UBS_ASSETS,"transactions": MAPPING_UBS_TXN},
    "bos":              {"assets": MAPPING_BOS_ASSETS,"transactions": MAPPING_BOS_TXN},
    "lgt":                {"assets": MAPPING_LGT_ASSETS,"transactions": MAPPING_LGT_TXN},
    "uob":                {"assets": MAPPING_UOB_ASSETS,"transactions": MAPPING_UOB_TXN},
    "jss":              {"assets": MAPPING_JSS_ASSETS,"transactions": MAPPING_JSS_TXN},
    "ubp":             {"assets": MAPPING_UBP_ASSETS,"transactions": MAPPING_UBP_TXN},
    "cai":                 {"assets": MAPPING_CAI_ASSETS,"transactions": MAPPING_CAI_TXN},
    "vpb":               {"assets": MAPPING_VPB_ASSETS,"transactions": MAPPING_VPB_TXN},
    "msp":              {"assets": MAPPING_MSP_ASSETS,"transactions": MAPPING_MSP_TXN},
    "ukh":              {"assets": MAPPING_UKH_ASSETS,"transactions": MAPPING_UKH_TXN},
    "other":              {"assets": MAPPING_OTHER_ASSETS,"transactions": MAPPING_OTHER_TXN},
}

def _render_column_block(mapping):
    header = ["| heading keyword ⭢ sub-heading key | json heading key ⭢ json sub-heading key | fields (keep order) |",
            "|-----------------------------------|------------------------------------------|---------------------|"]
    lines = []
    for heading, json_key, fields in mapping:
        fld = ", ".join(fields)
        lines.append(f"| `{heading}` | `{json_key}` | `{fld}` |")
    return "\n".join(header + lines)

def safe_format(base: str, **kwargs) -> str:
    # 1) temporarily protect placeholders we intend to keep
    placeholders = {k: f"__PLH_{k.upper()}__" for k in kwargs.keys()}
    for k, marker in placeholders.items():
        base = base.replace("{"+k+"}", marker)

    # 2) escape all remaining literal braces so .format won't touch them
    base = base.replace("{", "{{").replace("}", "}}")

    # 3) restore the real placeholders
    for k, marker in placeholders.items():
        base = base.replace(marker, "{"+k+"}")

    # 4) standard format
    return base.format(**kwargs)


header_EXTRACTOR = r"""
Inputs:
•⁠  ⁠*PDF* - the complete, multi-page statement provided in Step 0.

•⁠ Column headers for all tables:
{column_block}

1.⁠ ⁠From the provided bank statement table data and the list of separated column headers, reorganize the table as follows:
 - Detect asset-related tables and extract their visible header cells. For any composite/multi-header cell, split it into atomic headers strictly according to the provided separated headers list. Treat each atomic header as a distinct candidate.
 - For each detected table, select the most appropriate asset class from the provided header catalog using section/heading proximity and header-signature overlap. All subsequent mapping must use the canonical field set of the chosen asset class.
 - For every atomic PDF header, find exactly one best-matching canonical field in the chosen asset class. Use normalization (case/whitespace/punctuation tolerant), token/alias equivalence, and abbreviations handling. Do not translate labels and do not invent new fields.
    + If two PDF headers would map to the same canonical field, keep both mappings (record as duplicates), but the final header list must remain unique in canonical keys.
    + If confidence is marginal, still pick the single best match (ties resolved by semantic group consistency such as Cost vs. Market groups).
 - Do not extract data rows. Produce only the header mapping result. Enforce the canonical field order defined by fields (keep order) when listing the final matched headers.
 - Return valid JSON only (no prose/markdown/comments)
 
2.⁠ ⁠Output (headers-only, minimal; return valid JSON only)
 - Output valid JSON only: no explanatory text, Markdown, or comments.

 - Headers only: do not output any data rows.
 - Best match with de-duplication: each (post-split) PDF header must map to a single canonical field from the list; if multiple PDF headers map to the same canonical field, include it only once in the output.
 - Emit only actually present headers: list only canonical fields that were extracted from the PDF and successfully matched; fields that exist in the catalog but do not appear in the PDF must not be included.
 - Table-level filtering: omit any table that has no successfully matched headers.

{
  "tables": [
    {
      "table_name": "<table name or 'default'>",
      "headers": ["<canonical_field_1>", "<canonical_field_2>", "..."]
    }
  ]
}
"""

header_EXTRACTOR_V2 = r"""
Inputs:
• *PDF* - the complete, multi-page statement provided in Step 0.
• *Table Entries To Process* - a list of table occurrences, each with:
  {
    "page": <int>,
    "table_name": "<string>",
    "sub_table": "<string or null>"
  }
• *Column Headers (optional)* - pre-extracted headers for some or all table occurrences.
{column_block}

Processing Rules:

1. For EACH occurrence in *Table Entries To Process* (table_name, page):
   - Treat each occurrence as DISTINCT even if table_name is identical to another entry on a different page.
   - If Column Headers exist for this exact occurrence (most-specific match wins), use them as a reference.
   - Locate the table in the PDF on the specified page by matching table_name text. Include sub_table if provided to pinpoint the correct section.
   - Extract the table’s visible header cells from the PDF.
   - Merge reference headers and PDF-extracted headers, preserving order: reference headers first, then any new headers from PDF extraction.

2. Header Mapping:
   - Map each extracted atomic header to exactly one canonical field from the correct asset class in the header catalog.
   - Matching should be case/whitespace/punctuation tolerant, with alias and abbreviation awareness.
   - Do not invent fields or translate labels.
   - If multiple PDF headers map to the same canonical field, keep only one instance in the output list for that occurrence.
   - Follow the canonical field order from the header catalog.

3. Output Requirements:
   - Return a JSON object ONLY — no prose, Markdown, or comments.
   - Include ALL occurrences from *Table Entries To Process* in the given order.
   - If headers cannot be extracted or matched, output "headers": [] and add "status": "not_found" for that occurrence.
   - Schema:
     {
       "tables": [
         {
           "table_name": "<as provided>",
           "headers": ["<canonical_field_1>", "<canonical_field_2>", "..."],
           "status": "<ok | not_found>"
         }
       ]
     }
"""

def make_column_prompt(bank_name: str, kind: str) -> str:
    key = bank_name.lower().strip()
    if key not in BANK_MAPPINGS:
        key = "other"

    mapping_list = BANK_MAPPINGS[key][kind]
    #column_block = _render_column_block(mapping_list) if mapping_list else "*No mapping defined – please add one.*"
    return mapping_list