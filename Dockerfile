FROM python:latest
LABEL authors="emre"

EXPOSE 8000

WORKDIR /app

COPY ./requirements.txt /app
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./src /app

CMD ["uvicorn", "main:neodb", "--host", "0.0.0.0", "--port", "8000", "--reload"]
