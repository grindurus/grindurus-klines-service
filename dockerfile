# dockerfile

FROM python:3.13.7-slim

WORKDIR /app

COPY . .

RUN pip install -e .

EXPOSE 8000