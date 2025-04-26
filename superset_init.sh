#!/bin/bash

# Install additional Python packages
pip install redis flask-limiter psycopg2-binary

# Upgrade the database
superset db upgrade

# Create admin user
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin

# Initialize Superset
superset init

# Start Superset
superset run -p 8088 --with-threads --reload --debugger