FROM python:3.10.2

LABEL maintainer="JM Vazquez"

ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN pip3 install --upgrade setuptools && pip3 install pip virtualenv pipenv

VOLUME /code
WORKDIR /code
