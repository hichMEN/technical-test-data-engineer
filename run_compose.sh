#!/bin/bash

#deploy containers
docker-compose up --d

#run tests manually
docker compose exec app pytest /app

#exec pipeline manually
#docker compose exec app python /app/src/etl/main.py  --config_path /app/src/etl/etl_config.yaml
docker compose exec app sh
cd ../../src/etl
python main.py
