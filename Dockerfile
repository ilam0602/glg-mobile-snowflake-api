# Use a small Python base image
FROM python:3.10-slim

# Set your working directory
WORKDIR /app

# Copy dependency list and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code (including start_server.sh)
COPY . .

# Make your script executable
RUN chmod +x ./start_flask.sh

# Expose port Gunicorn will listen on
ENV PORT 8080
EXPOSE 8080

# Use your existing script to start the server
CMD ["./start_flask.sh"]
