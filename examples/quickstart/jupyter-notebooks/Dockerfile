#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
# This Dockerfile creates a custom Docker image for Jupyter
# to use with the Apache Druid Jupyter notebook tutorials.
# Build using `docker build -t imply/druid-notebook:latest .`
# -------------------------------------------------------------

# Use the Jupyter base notebook as the base image
# Copyright (c) Project Jupyter Contributors.
# Distributed under the terms of the 3-Clause BSD License.
FROM jupyter/base-notebook

# Set the container working directory
WORKDIR /home/jovyan

# Install required Python packages
RUN pip install requests \
    pip install pandas \
    pip install numpy \
    pip install seaborn \
    pip install bokeh \
    pip install kafka-python \
    pip install sortedcontainers 

# Install druidapi client from apache/druid
# Local install requires sudo privileges 
USER root
ADD druidapi /home/jovyan/druidapi
WORKDIR /home/jovyan/druidapi
RUN pip install .


# Import data generator and configuration file
# Change permissions to allow import (requires sudo privileges)
RUN mkdir -p /home/jovyan/notebooks
# RUN mkdir -p /home/jovyan/notebooks/"1. Introduction"
# WORKDIR /home/jovyan/notebooks/"1. Introduction"

# WIP -- change to apache repo
# ADD https://raw.githubusercontent.com/shallada/druid/data-generator/examples/quickstart/jupyter-notebooks/data-generator/DruidDataDriver.py .
# ADD docker-jupyter/kafka_docker_config.json .
# RUN chmod 664 DruidDataDriver.py 
# RUN chmod 664 kafka_docker_config.json

WORKDIR /home/jovyan/work
USER jovyan

# Copy the Jupyter notebook tutorials including folder structure from the
# build directory to the image working directory 
# COPY ./notebooks/ ./


# Add location of the data generator to PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/home/jovyan/notebooks/Introduction"

