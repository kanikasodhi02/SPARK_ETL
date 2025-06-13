FROM python:3.9.1

# Install required tools
RUN apt-get update && apt-get install -y curl wget

# Copy project files
COPY ./ ./

# Set environment variables for versions
ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    JAVA_VERSION=11

# Set Java environment variables
ENV JAVA_HOME="/home/jdk-${JAVA_VERSION}.0.2"
ENV PATH="${JAVA_HOME}/bin/:${PATH}"

# Download and install Java 11
RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk${JAVA_VERSION}/9/GPL/openjdk-${JAVA_VERSION}.0.2_linux-x64_bin.tar.gz" \
   && TMP_DIR="$(mktemp -d)" \
   && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk-${JAVA_VERSION}.0.2_linux-x64_bin.tar.gz" \
   && mkdir -p "${JAVA_HOME}" \
   && tar xzf "${TMP_DIR}/openjdk-${JAVA_VERSION}.0.2_linux-x64_bin.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
   && rm -rf "${TMP_DIR}" \
   && java --version

# Download and install Spark 3.5.1 with Hadoop 3 + Scala 2.13
RUN DOWNLOAD_URL_SPARK="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz" \
    && wget --no-verbose -O apache-spark.tgz "${DOWNLOAD_URL_SPARK}" \
    && mkdir -p /home/spark \
    && tar -xf apache-spark.tgz -C /home/spark --strip-components=1 \
    && rm apache-spark.tgz

# Set Spark environment variables
ENV SPARK_HOME="/home/spark"
ENV PATH="${SPARK_HOME}/bin/:${PATH}"

# Set PySpark environment variables
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Run unit tests
RUN python -m pytest -s ./test/unit_test/test_*.py

# Run main application
ENTRYPOINT ["python", "./src/main.py"]
