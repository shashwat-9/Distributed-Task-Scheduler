FROM ubuntu:22.04

ARG AWS_CLI="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"

LABEL author="Shashwat Mishra"
LABEL mode="python-3-worker"

# Install python and aws cli
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv curl unzip && \
    curl ${AWS_CLI} -o "awscli.zip" && \
    unzip awscli.zip && \
    ./aws/install && \
    rm -rf awscli.zip aws/ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN python3 -m pip install --upgrade pip

# Create Structure and verify installation
RUN mkdir -p /app /app/output && \
    python3 --version && \
    aws --version

# COPY execute.sh inside the app directory
COPY scripts/execute.sh /app

#Create group and user
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

#Ownership and permissions changed to appuser
RUN chown -R appuser:appuser /app && \
    chmod -R 550 /app

USER appuser

WORKDIR /app
