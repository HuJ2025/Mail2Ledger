import json, tempfile, requests, os, logging

from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from uuid import uuid4
from app.agents.workflow import pdf_finder

webhook_bp = Blueprint("webhook", __name__)

@webhook_bp.route("/ledger-sync", methods=["POST", "OPTIONS"])
def ledger_sync():
    if request.method == "OPTIONS":
        return "", 200

    # ------- ① 解析请求 -------
    try:
        payload      = request.get_json(force=True)
        file_url:str = payload["fileUrl"]          # 单个 URL
        print("file_url: ", file_url)
    except (BadRequest, KeyError):
        return jsonify(error="JSON 格式错误，应包含 fileUrl"), 400

    # ------- ② 下载 PDF -------
    try:
        r = requests.get(file_url, timeout=90)
        r.raise_for_status()
    except requests.RequestException as err:
        return jsonify(error=f"下载失败: {err}"), 502


    try:
        success = pdf_finder(r)   # 这里会调用 OpenAI OCR → 写 Excel
    except Exception as err:
        logging.exception("import_workflow crashed")
        return jsonify(error=f"处理失败: {err}"), 500

    # ------- ④ 返回结果给前端 -------
    return jsonify(
        status   = "success" if success else "fail",
    ), 200