#!/bin/bash

set -e

echo "Setting up E-Commerce ELT Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker and try again."
    exit 1
fi

echo "Docker is running"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ".env file created"
else
    echo ".env file already exists"
fi

source .env

echo "Starting Docker services..."
make up

echo "Waiting for services to be healthy..."
sleep 10

echo "Service Status:"
make ps

echo "Setup complete!"

echo "Seeding source database..."
uv run seed_source_db.py
echo "Seeding complete!"
