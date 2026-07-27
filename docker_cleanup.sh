#!/bin/bash
# Script to cleanly kill and wipe Docker containers on Linux
# Useful for completely resetting the local testing environment.

echo "Stopping all running Docker containers..."
docker stop $(docker ps -aq) 2>/dev/null || echo "No running containers to stop."

echo "Removing all Docker containers..."
docker rm $(docker ps -aq) 2>/dev/null || echo "No containers to remove."

# Optional: If you want to also prune networks and volumes, uncomment the following line:
# echo "Pruning Docker system (removing unused networks and volumes)..."
# docker system prune -a --volumes -f

echo "Done. You can now spin up new containers using 'docker compose up -d'."
