FROM python:3.9-slim

WORKDIR /app

# Copy the requirements.txt before other files to leverage Docker cache
COPY requirements.txt .

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 80

ENV NAME World

CMD ["python", "datasets.py"]

