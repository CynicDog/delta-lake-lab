FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu

USER root

# Install essential tools and SDP
RUN apt-get update && \
    apt-get install -y tree curl && \
    pip install --no-cache-dir "pyspark[pipelines]==4.1.0"

# Setup workspace
WORKDIR /app
RUN mkdir -p /app/sdp/pipeline-storage /app/sdp/checkpoints /app/sdp/transformations

# Ensure spark user owns the app directory
RUN chown -R spark:spark /app

USER spark