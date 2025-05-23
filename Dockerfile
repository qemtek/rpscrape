FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies in a single layer to reduce image size
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
COPY requirements.txt requirements.in ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY RPScraper/ /app/RPScraper/
COPY tests/ /app/tests/

# Set environment variables
ENV PROJECTSPATH=/app/RPScraper
ENV PYTHONPATH=/app

# Set execute permissions for scripts
RUN chmod +x RPScraper/scripts/run_daily_updates.sh

# Default command to run daily updates
CMD ["/app/RPScraper/scripts/run_daily_updates.sh"]
