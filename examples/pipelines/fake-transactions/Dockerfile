ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
RUN pip install --no-cache-dir "duckdb>=1.3,<3" "boto3>=1.35,<2"
COPY generate.py /app/generate.py
USER 1000
ENTRYPOINT ["python", "/app/generate.py"]
