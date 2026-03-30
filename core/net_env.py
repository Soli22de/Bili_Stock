import os
from contextlib import contextmanager


@contextmanager
def no_proxy_env(enabled=False):
    if not enabled:
        yield
        return
    keys = ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY")
    backup = {k: os.environ.get(k) for k in keys}
    try:
        for key in keys:
            os.environ.pop(key, None)
        yield
    finally:
        for key, value in backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
