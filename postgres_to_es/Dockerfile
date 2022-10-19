FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /opt/app

ENV ES_HOST es
ENV ES_PORT 9200
ENV ES_INDEX movies

COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y netcat

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN groupadd -r app && useradd -d /opt/app -r -g app app \
    && chown app:app -R /opt/app

USER app

ENTRYPOINT [ "bash", "-c", "config/start.sh"]