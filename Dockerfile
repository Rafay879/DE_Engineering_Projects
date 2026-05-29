FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /dbt

# Install dbt with Athena adapter
RUN pip install --no-cache-dir \
    dbt-athena-community==1.8.4 \
    boto3==1.34.0

# Copy dbt project files
COPY dbt_project.yml .
COPY packages.yml .
COPY profiles_docker.yml ./profiles.yml
COPY models/ ./models/
COPY snapshots/ ./snapshots/
COPY tests/ ./tests/
COPY macros/ ./macros/
COPY entrypoint.sh .

# Install dbt packages (dbt_utils)
RUN dbt deps

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Default command
ENTRYPOINT ["./entrypoint.sh"]