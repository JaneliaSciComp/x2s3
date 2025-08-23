FROM python:3.12
ARG GIT_TAG=main

ENV HOST=0.0.0.0
ENV PORT=8000
ENV NUM_WORKERS=8

WORKDIR /app
RUN git clone --branch $GIT_TAG --depth 1 https://github.com/JaneliaSciComp/x2s3.git

WORKDIR /app/x2s3

RUN pip install --no-cache-dir -e .

CMD uvicorn x2s3.app:app --host ${HOST} --port ${PORT} --workers ${NUM_WORKERS} --access-log --forwarded-allow-ips='*' --proxy-headers

