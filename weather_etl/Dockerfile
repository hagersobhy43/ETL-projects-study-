# Use debian:12-slim image as the base
FROM debian:12-slim

# Prevent interactive prompts during package installation (important for automation)
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install necessary tools
# curl: for data extraction
# cron: for scheduling tasks
# nano: for editing files
# tzdata: for managing time zones
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        cron \
        nano \
        tzdata && \
    rm -rf /var/lib/apt/lists/*

# Set the timezone to Egypt/Cairo automatically
RUN ln -fs /usr/share/zoneinfo/Africa/Cairo /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata

# Set the working directory inside the container to /app
WORKDIR /app

# Start the cron service and keep the container running in the foreground
# The "tail -f /dev/null" part prevents the container from exiting immediately
CMD service cron start && tail -f /dev/null
