#!/bin/bash
set -e

# One-time setup: shared docker network so LocalStack's Lambda containers
# can reach the local Postgres container by hostname ("postgres-local").
echo "[INFO] Ensuring shared docker network 'pipeline-net' exists..."
docker network create pipeline-net 2>/dev/null || echo "[INFO] Network already exists, continuing..."

echo "[INFO] Starting local Postgres + Superset (docker-compose.local.yml)..."
docker compose -f docker-compose.local.yml up -d

echo ""
echo "[SUCCESS] Postgres is on localhost:5432 (analytics_user/analytics_pass/gold_analytics)."
echo "[SUCCESS] Superset UI will be ready in a minute or two at: http://localhost:8088 (admin/admin)"
echo ""
echo "[IMPORTANT] For LoadToPostgresFunction (running inside LocalStack) to reach postgres-local,"
echo "LocalStack itself must be attached to 'pipeline-net' and its Lambda containers must join it."
echo "If you start LocalStack via the CLI, run it like this:"
echo ""
echo "  LAMBDA_DOCKER_NETWORK=pipeline-net localstack start -d"
echo ""
echo "If you use docker-compose for LocalStack, add to its service definition:"
echo "  networks: [pipeline-net]"
echo "  environment: [\"LAMBDA_DOCKER_NETWORK=pipeline-net\"]"