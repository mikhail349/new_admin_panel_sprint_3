FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 4
ENV UWSGI_THREADS 8
ENV UWSGI_HARAKIRI 60
ENV DJANGO_SETTINGS_MODULE 'config.settings'

ENV DB_HOST db
ENV DB_PORT 5432

COPY requirements.txt requirements.txt
COPY uwsgi/uwsgi.ini uwsgi.ini

RUN apt-get update && apt-get install -y netcat

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN python manage.py collectstatic --noinput 

EXPOSE 8000

RUN groupadd -r app && useradd -d /opt/app -r -g app app \
    && chown app:app -R /opt/app

USER app

ENTRYPOINT [ "bash", "-c", "commands/start.sh"]