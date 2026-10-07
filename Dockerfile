FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STATE_DB=/data/bot.sqlite3

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py live.py ./
COPY tests ./tests

# Offline suites (no network, no token, no key): a failing test fails the build instead of shipping.
# The headless-browser check in tests/test_web.py is skipped here because Chromium is not installed.
RUN python -m py_compile main.py live.py && python tests/run_all.py && rm -rf tests

CMD ["python", "-u", "main.py"]
