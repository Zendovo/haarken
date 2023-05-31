# syntax=docker/dockerfile:1

FROM python:3

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y netcat

ENV APP_HOME=/home/app/web

WORKDIR $APP_HOME

RUN mkdir -p $APP_HOME/staticfiles
RUN mkdir -p $APP_HOME/media
COPY requirements.txt $APP_HOME

RUN pip install -r requirements.txt

COPY . $APP_HOME

ENTRYPOINT [ "/home/app/web/entrypoint.sh" ]
