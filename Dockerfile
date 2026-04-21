FROM python:3.11-slim
RUN pip install tinydb

WORKDIR /app