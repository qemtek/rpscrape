FROM python:3.7-slim-buster

RUN apt-get update
RUN apt-get update && apt-get -y install gcc
RUN apt-get install --reinstall build-essential -y
RUN pip install --upgrade pip

ADD requirements.txt /
ADD requirements.in /

RUN mkdir -p /RPScraper
COPY RPScraper /RPScraper
RUN mkdir -p /tests
COPY tests /tests

ENV PROJECTSPATH /RPScraper

RUN pip3 install -r requirements.txt
RUN chmod +x RPScraper/scripts/full_refresh.sh
RUN chmod +x RPScraper/scripts/run_daily_updates.sh

ENV PROJECTSPATH /RPScraper
ENV PYTHONPATH /


# ENTRYPOINT /RPScraper/scripts/full_refresh.sh
