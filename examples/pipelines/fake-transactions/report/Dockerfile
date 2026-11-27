ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
RUN pip install --no-cache-dir "streamlit>=1.40,<2" "duckdb>=1.3,<3" "boto3>=1.35,<2"
COPY app.py /app/app.py
# The root filesystem is read-only; Streamlit keeps its state under $HOME.
ENV HOME=/tmp
USER 1000
# pipeyard supplies PORT (where to listen) and BASE_PATH (the URL prefix it proxies from).
ENTRYPOINT ["sh", "-c", "exec streamlit run /app/app.py --server.port \"$PORT\" --server.address 0.0.0.0 --server.baseUrlPath \"$BASE_PATH\" --server.headless true --server.enableCORS false --server.enableXsrfProtection false --server.fileWatcherType none --client.toolbarMode minimal --browser.gatherUsageStats false"]
