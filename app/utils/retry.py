import time, functools, logging, httpx, openai

RETRYABLE = (httpx.HTTPError, openai.OpenAIError)
def retry(attempts=3, backoff=2.0):
    """
    裝飾器：遇到網路類錯誤就重試，指數退避 (2-4-8…s)。
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except RETRYABLE as err:
                    logging.warning("🛑 %s failed (%s/%s) – %s",
                                    func.__name__, i, attempts, err)
                    if i == attempts:            # 最後一次也失敗 → 讓上層處理
                        raise
                    time.sleep(backoff ** i)
        return wrapper
    return decorator