#!/usr/bin/env bash

# Create directories
mkdir -p ./rabbitmq/{data,log}

# Set environment variables
export RABBITMQ_MNESIA_BASE=$(pwd)/rabbitmq/data
export RABBITMQ_LOG_BASE=$(pwd)/rabbitmq/log

# Start RabbitMQ
rabbitmq-server