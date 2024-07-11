FROM python:3.9
ARG GIT_TAG=main

WORKDIR /app
RUN git clone --branch $GIT_TAG --depth 1 https://github.com/JaneliaSciComp/jproxy.git

WORKDIR /app/jproxy

RUN pip install --no-cache-dir --upgrade -r requirements.txt

CMD ["uvicorn", "jproxy.serve:app", \
    "--host", "0.0.0.0", \
    "--port", "8000", \
    "--workers", "8", \
    "--access-log", \
    "--ssl-keyfile", "/certs/default.key", \
    "--ssl-certfile", "/certs/default.crt" \
]
