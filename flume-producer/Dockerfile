FROM eclipse-temurin:8-jdk-focal

ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV PATH=$FLUME_HOME/bin:$PATH

# Install dependencies
RUN apt-get update && apt-get install -y wget curl && rm -rf /var/lib/apt/lists/*

# Download and install Flume
RUN wget -q "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" -O /tmp/flume.tar.gz \
    && tar -xzf /tmp/flume.tar.gz -C /opt \
    && mv /opt/apache-flume-${FLUME_VERSION}-bin $FLUME_HOME \
    && rm /tmp/flume.tar.gz

# Copy Kafka client jars into Flume lib (needed for Kafka sink)
RUN wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar" -O $FLUME_HOME/lib/kafka-clients-3.4.0.jar

WORKDIR $FLUME_HOME

CMD ["flume-ng", "agent", \
     "--conf", "/opt/flume/conf", \
     "--conf-file", "/opt/flume/conf/flume.conf", \
     "--name", "producer", \
     "-Dflume.root.logger=INFO,console"]
