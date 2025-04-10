#!/bin/bash

# remove containers antigos
docker rm -f worker-processor 2>/dev/null || true
docker rmi -f worker-processor-worker_processor 2>/dev/null || true

docker network create net

docker compose build
