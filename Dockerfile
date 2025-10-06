# Use a stable Debian base that includes OpenJDK packages
FROM python:3.10-slim-bookworm

# Install OpenJDK 17 (Bookworm has this version)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre wget curl && \
    wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar xf spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR /app
COPY . /app

RUN pip install pyspark

CMD ["bash"]
