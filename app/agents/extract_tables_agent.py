# app/agents/extract_tables_agent.py

import json
import logging
from app.utils.retry import retry;
from app.services.pdf.pdf_service import upload_pdf2openai, upload_pdf2openai_no_ocr, delete_openai_file, slice_pdf, extract_pages
from app.prompts.bank_prompt import pdf_DATAMAP_PROMPT, BANK_INFO_EXTRACTION_PROMPT, table_EXTRACTION_PROMPT, COLUMN_HEADER_EXTRACTION_PROMPT
from app.prompts.bank_prompt import STOCK_EXTRACTOR_JSON_PROMPT

from app.utils.common import clean_json_markdown, generate_excel_report, to_section_format, replace_nullish_strings
from app.prompts.prompt_gen import make_column_prompt
from app.utils.openai_client import get_agent_response


#Step 1: Get bank info
@retry(max_retries=10)
def get_bank_info(file_id: str):
    resp = get_agent_response(
        file_id=file_id,
        model="gpt-5-mini",
        instructions= BANK_INFO_EXTRACTION_PROMPT,
        retry=10
    )

    logging.info(clean_json_markdown(resp))
    resp = json.loads(clean_json_markdown(resp))
    return resp.get("bank_name"), resp.get("statement_date"), resp.get("account_numbers"), resp.get("base_currency")

#Step 2: Classify statement
@retry(max_retries=10)
def classify_statement(file_id: str):
    resp = get_agent_response(
        file_id=file_id,
        instructions = table_EXTRACTION_PROMPT,
        model="gpt-5",
        retry=10
    )
    logging.info(clean_json_markdown(resp))
    resp = json.loads(clean_json_markdown(resp))
    return resp.get("asset_tables"), resp.get("transaction_tables")

#Step 3: Get column headers
@retry(max_retries=10)
def get_column_headers(file_id: str, extracted: str, instruction: str):
    resp = get_agent_response(
        file_id=file_id,
        user_input=extracted,
        model="gpt-5",
        instructions= instruction,
        retry=10,
    )
    logging.info(clean_json_markdown(resp))
    return json.loads(clean_json_markdown(resp))

#Step 4: Get table content
@retry(max_retries=10)
def get_tablecontent(file_id: str, column_headers: str):
    resp = get_agent_response(
        file_id=file_id,
        user_input= column_headers,
        instructions=pdf_DATAMAP_PROMPT,
        model = "gpt-5",
        retry=10
    )
    resp = replace_nullish_strings(resp)
    logging.info(clean_json_markdown(resp))
    return json.loads(clean_json_markdown(resp))

#Step 5: Audit (not using right now)
# @retry(max_retries=10)
# def audit_extraction_quality(file_id: str,extracted_json: dict | str, instruction: str):
#     resp = get_agent_response(
#         model="gpt-5",
#         instructions=instruction,
#         file_id=file_id,
#         user_input= extracted_json
#     )
#     logging.info("Audit response: %s", clean_json_markdown(resp))
#     return json.loads(clean_json_markdown(resp))

@retry(max_retries=10)
def extract_assets(file_id: str, bank_name: str, extracted: str):

    column_template = make_column_prompt(bank_name, "assets")
    print("header template", column_template)
    # create header template with table names as usr input
    user_input = {"table_names": extracted, "default_headers": column_template}
    usr_inputs_str = user_input if isinstance(user_input, str) else json.dumps(user_input, ensure_ascii=False)
    
    column_headers= get_column_headers(file_id, usr_inputs_str, COLUMN_HEADER_EXTRACTION_PROMPT)

    response = get_tablecontent(file_id, column_headers)
    
    if not response:
        return {"Assets": []}

    return response

@retry(max_retries=10)
def extract_transactions(file_id: str, bank_name: str, extracted: str):

    column_template = make_column_prompt(bank_name, "transactions")
    
    # create header template with table names as usr input
    user_input = {"table_names": extracted, "default_headers": column_template}
    usr_inputs_str = user_input if isinstance(user_input, str) else json.dumps(user_input, ensure_ascii=False)
    
    column_headers= get_column_headers(file_id, usr_inputs_str, COLUMN_HEADER_EXTRACTION_PROMPT)
    
    response = get_tablecontent(file_id, column_headers)

    if not response:
        return {"Transactions": []}
    
    return response

def run_assets_vs_txn_workflow(pdf_url: str):

    # upload pdf
    #full_file_id, ocr_bytes = upload_pdf2openai(pdf_url)
    full_file_id, ocr_bytes = upload_pdf2openai_no_ocr(pdf_url)
    # get structural data
    bank_name, as_of_date, account_number, base_currency = get_bank_info(full_file_id)
    assets, transactions = classify_statement(full_file_id)
    
    #extract assets and transactions
    txn_raw = extract_transactions(full_file_id, bank_name, str(transactions))
    assets_raw = extract_assets(full_file_id, bank_name, str(assets))

    # Excel/report needs section view; DB needs universal view
    assets_excel = to_section_format(assets_raw, "Assets")
    txn_excel    = to_section_format(txn_raw, "Transactions")

    bank_data = {
        "Assets":       assets_excel.get("Assets"),
        "Transactions": txn_excel.get("Transactions"),
    }
    excel_s3_url = generate_excel_report(bank_data, bank_name)

    delete_openai_file(full_file_id)

    # ✅ return universal for DB ingestion
    return {
        "bank_name"        : bank_name,
        "account_number"   : account_number,
        "base_currency"    : base_currency,
        "assets"           : assets_raw,
        "transactions"     : txn_raw,
        "excel_report_url" : excel_s3_url,
        "pdf_url"          : pdf_url,
        "as_of_date"       : as_of_date,
    }

def stock_analysis_process(data, currency):
    """
    stock_analysis_process:
    Extract stock information for each bank using openai_assistant_call.
    """
    prompt = STOCK_EXTRACTOR_JSON_PROMPT.replace("{{base_currency}}", currency)

    response = get_agent_response(data, prompt)

    print("Stock Analysis Response:", response)

    reformat_response = response
    stock_response = json.loads(reformat_response)
    return stock_response