FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /opt/app

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN groupadd -r app && useradd -d /opt/app -r -g app app \
    && chown app:app -R /opt/app

USER app

ENTRYPOINT [ "python", "app/main.py"]