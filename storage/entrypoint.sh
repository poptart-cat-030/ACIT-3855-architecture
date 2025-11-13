#!/bin/sh

echo "Running create_tables.py..."
python3 create_tables.py

echo "Starting service..."
python3 app.py