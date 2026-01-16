# Base image to use (swap out if you need to use another Python version (e.g. 3.8, 3.9, 3.10, etc.)
FROM python:3.10-slim

LABEL maintainer="Naomi Du Naomi.du@pitchbook.com" \
      version="1.0.0"

# create the pipe user
RUN useradd -r -m -U pipeuser

# Argument to enable installing PBLabs/BI Python packages
ARG pypi_index

# SET LOCAL as the environment, this will change via Airflow environment variable injection
ENV ORCHESTRATOR_ENV=local

WORKDIR /core

# build-essential may be necessary for certain packages which require compilation
# RUN apt-get update && \
# 	apt-get install -y --no-install-recommends build-essential

# Copy requirements first so that we don't have to reinstall dependencies for every code change
COPY --chown=pipeuser:pipeuser requirements.txt /core

# Python package installs
RUN pip install --no-cache-dir --extra-index-url ${pypi_index} -r requirements.txt && \
    rm requirements.txt

# Copy all code into container, currently exclude notebook in .dockerignore
COPY --chown=pipeuser:pipeuser core/ /core/

# change to the pipe user
USER pipeuser
