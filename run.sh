# Run Docker Compose to build and start the containers
docker compose up --build -d

# Execute a command inside the spark-master container
docker exec -it spark-master bash -c 'python3 /opt/bitnami/spark/main.py'
