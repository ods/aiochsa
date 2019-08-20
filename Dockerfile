FROM python:3.7.4

RUN pip install --no-cache-dir poetry
RUN poetry config settings.virtualenvs.create false

RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -o /usr/local/bin/wait-for-it.sh \
    && chmod 755 /usr/local/bin/wait-for-it.sh

RUN mkdir -p /usr/src
WORKDIR /usr/src

COPY poetry.lock pyproject.toml /usr/src/
RUN poetry install

CMD wait-for-it.sh ch:8123 -- ./test.py
