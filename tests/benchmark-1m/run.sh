#!/bin/bash
set -e

echo "Building and starting containers..."
docker compose up --build -d oxidb mongodb

echo "Waiting for services to be healthy..."
docker compose up --build --abort-on-container-exit bench

echo ""
echo "Cleaning up..."
docker compose down -v
