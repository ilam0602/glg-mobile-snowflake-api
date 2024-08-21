#!/bin/bash

# Start Gunicorn with timeout for fault tolerance
# gunicorn --bind 0.0.0.0:8080 --timeout 15 --preload_app=True --max-requests=1000 --max-requests-jitter=100 --workers=2 --log-level=debug wsgi:app
gunicorn --bind 0.0.0.0:8080 --timeout 20  --preload --max-requests 70 --max-requests-jitter=20 --workers=3 --log-level=debug wsgi:app

echo "Server is running..."