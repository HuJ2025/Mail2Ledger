# run_realtime_worker.py

import os
import time
import logging
from pathlib import Path

from app.email.ingest.pipeline import run_once

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def main():
    ROOT = Path(__file__).resolve().parent
    os.chdir(ROOT)

    # 基础轮询间隔（秒）
    base_interval = int(os.getenv("MAIL2LEDGER_POLL_SECONDS", "60"))
    # 空转时最大退避（秒）
    max_interval = int(os.getenv("MAIL2LEDGER_MAX_POLL_SECONDS", "600"))

    sleep_s = base_interval

    logging.info(f"[worker] started. base_interval={base_interval}s max_interval={max_interval}s")

    while True:
        try:
            stats = run_once()  # ✅ 会从 env 读取 labels/client_id/bank 等配置
            logging.info(f"[worker] stats={stats}")

            processed = int(stats.get("emails_processed", 0))
            failed = int(stats.get("emails_failed", 0))
            skipped = int(stats.get("emails_skipped", 0))

            # 若这轮没做事（没处理、没失败、没跳过），就退避；否则恢复基础间隔
            if processed == 0 and failed == 0 and skipped == 0:
                sleep_s = min(sleep_s * 2, max_interval)
            else:
                sleep_s = base_interval

        except Exception as e:
            logging.exception(f"[worker] run_once crashed: {e}")
            # 崩了也不要疯狂重试
            sleep_s = min(max_interval, max(base_interval, sleep_s))

        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
