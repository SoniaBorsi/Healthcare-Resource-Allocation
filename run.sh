docker compose up --build -d

docker exec -it spark-master bash -c 'python3 /opt/bitnami/spark/main.py'
