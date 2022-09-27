FROM python:3.8-slim-buster

RUN apt-get update && apt-get install
RUN apt-get install -y \
    libpq-dev \
    gcc \
    && apt-get clean
RUN python -m pip install --upgrade pip
WORKDIR /app
COPY requirements.txt requirements.txt
RUN python -m pip install -r requirements.txt
COPY . .
CMD ["python3", "main.py"]