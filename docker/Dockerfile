FROM python:3.9
ARG GIT_TAG=main

ENV HOST=0.0.0.0
ENV PORT=8000
ENV NUM_WORKERS=8

WORKDIR /app
RUN git clone --branch $GIT_TAG --depth 1 https://github.com/JaneliaSciComp/jproxy.git

WORKDIR /app/jproxy

RUN pip install --no-cache-dir --upgrade -r requirements.txt

CMD uvicorn jproxy.serve:app --host ${HOST} --port ${PORT} --workers ${NUM_WORKERS} --access-log --forwarded-allow-ips='*' --proxy-headers

