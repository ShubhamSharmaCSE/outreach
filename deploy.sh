#!/bin/bash

# Deployment script for the sync service

set -e

echo "🚀 Starting deployment of Bi-Directional Sync Service..."

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose >/dev/null 2>&1; then
    echo "❌ Docker Compose is not installed. Please install it and try again."
    exit 1
fi

# Pull latest images
echo "📥 Pulling latest Docker images..."
docker-compose pull

# Build the application
echo "🔨 Building application..."
docker-compose build

# Start Redis first
echo "🔴 Starting Redis..."
docker-compose up -d redis

# Wait for Redis to be healthy
echo "⏳ Waiting for Redis to be ready..."
until docker-compose exec redis redis-cli ping >/dev/null 2>&1; do
    echo "   Redis is not ready yet, waiting..."
    sleep 2
done
echo "✅ Redis is ready!"

# Start the sync service
echo "🔄 Starting sync service..."
docker-compose up -d sync-service

# Wait for sync service to be healthy
echo "⏳ Waiting for sync service to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -f http://localhost:8000/health >/dev/null 2>&1; then
        echo "✅ Sync service is ready!"
        break
    fi
    echo "   Sync service is not ready yet, waiting... (attempt $((attempt+1))/$max_attempts)"
    sleep 5
    attempt=$((attempt+1))
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ Sync service failed to start within expected time"
    echo "📋 Service logs:"
    docker-compose logs sync-service
    exit 1
fi

# Start worker processes
echo "👷 Starting worker processes..."
docker-compose up -d sync-worker

# Start monitoring (optional)
read -p "🔍 Do you want to start monitoring services (Prometheus + Grafana)? [y/N]: " start_monitoring
if [[ $start_monitoring =~ ^[Yy]$ ]]; then
    echo "📊 Starting monitoring services..."
    docker-compose up -d prometheus grafana
    echo "✅ Monitoring services started!"
    echo "   📊 Prometheus: http://localhost:9090"
    echo "   📈 Grafana: http://localhost:3000 (admin/admin)"
fi

# Display status
echo ""
echo "🎉 Deployment completed successfully!"
echo ""
echo "📋 Service Status:"
docker-compose ps

echo ""
echo "🔗 Service URLs:"
echo "   🔄 Sync Service API: http://localhost:8000"
echo "   📚 API Documentation: http://localhost:8000/docs"
echo "   ❤️  Health Check: http://localhost:8000/health"
echo "   📊 Metrics: http://localhost:8000/metrics/prometheus"

echo ""
echo "🛠️  Management Commands:"
echo "   View logs: docker-compose logs -f"
echo "   Scale workers: docker-compose up -d --scale sync-worker=5"
echo "   Stop services: docker-compose down"
echo "   View metrics: curl http://localhost:8000/metrics/queue"

echo ""
echo "✅ Sync service is now running and ready to handle requests!"
