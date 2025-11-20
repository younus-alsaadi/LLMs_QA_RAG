FROM ghcr.io/astral-sh/uv:0.6.14-python3.10-bookworm

WORKDIR /app

# Install additional system dependencies for lxml and other packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libavif-dev pkg-config \
    libjpeg-dev \
    gcc unzip zip \
    python3-dev \
    libxml2-dev \
    libxslt1-dev \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*


COPY src/requirements.txt .
RUN uv pip install -r requirements.txt --system

# Put the code under /app/src (not flattened)
COPY src/ /app/src

# Put alembic.ini alongside your code
COPY docker/rag_pipeline/alembic.example.ini /app/src/models/db_schemes/rag_qa/alembic.ini

COPY docker/rag_pipeline/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

# If main.py lives in src/main.py, update the app import path:
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]