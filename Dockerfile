FROM python:3.10-alpine

WORKDIR /usr/app/src
COPY main.py ./
COPY process_lsl.py ./config/

RUN \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 python3 -m pip install psycopg2 --no-cache-dir && \
 apk --purge del .build-deps

CMD [ "python", "./postgres_connection_test.py"]
