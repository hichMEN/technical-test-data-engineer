# Use the official Python image from the Docker Hub
FROM ubuntu:20.04 AS builder-image
# avoid stuck build due to user prompt
ARG DEBIAN_FRONTEND=noninteractive

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container
COPY . .

# Install the dependencies
RUN apt-get update && apt-get install --no-install-recommends -y python3.9 python3.9-dev python3.9-venv python3-pip python3-wheel build-essential && \
	apt-get clean && rm -rf /var/lib/apt/lists/*
RUN python3.9 -m venv /home/myuser/venv
ENV PATH="/home/myuser/venv/bin:$PATH"    
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir wheel
RUN pip3 install --no-cache-dir -r requirements.txt

# RUN fast api
EXPOSE 8000
WORKDIR /app/src/moovitamix_fastapi
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]