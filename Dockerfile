# 辐射实验舱维护事务锁管理服务
# 仅依赖 Python 3.11 标准库，构建无需联网安装依赖
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DATA_DIR=/data

WORKDIR /app

COPY app ./app
COPY tests ./tests
COPY scripts ./scripts

RUN mkdir -p /data && python -m py_compile app/*.py tests/*.py scripts/verify.py

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2).status == 200 else 1)"

CMD ["python", "-m", "app.server"]
