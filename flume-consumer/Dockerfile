FROM eclipse-temurin:8-jdk-focal

ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV HADOOP_VERSION=3.2.1
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$FLUME_HOME/bin:$HADOOP_HOME/bin:$PATH

RUN apt-get update && apt-get install -y wget curl && rm -rf /var/lib/apt/lists/*

# Install Flume
RUN wget -q "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" -O /tmp/flume.tar.gz \
    && tar -xzf /tmp/flume.tar.gz -C /opt \
    && mv /opt/apache-flume-${FLUME_VERSION}-bin $FLUME_HOME \
    && rm /tmp/flume.tar.gz

# Install Hadoop client (for HDFS sink)
RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -O /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
    && rm /tmp/hadoop.tar.gz

# Kafka client jar (for Kafka source)
RUN wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar" -O $FLUME_HOME/lib/kafka-clients-3.4.0.jar

# Copy Hadoop HDFS jars into Flume lib
RUN cp $HADOOP_HOME/share/hadoop/common/*.jar $FLUME_HOME/lib/ && \
    cp $HADOOP_HOME/share/hadoop/common/lib/*.jar $FLUME_HOME/lib/ && \
    cp $HADOOP_HOME/share/hadoop/hdfs/*.jar $FLUME_HOME/lib/ && \
    cp $HADOOP_HOME/share/hadoop/hdfs/lib/*.jar $FLUME_HOME/lib/ 2>/dev/null || true

# ──  GUAVA CLASH ──────────────────────────────────
# Remove old or duplicated guava jar versions that cause NoSuchMethodError
RUN rm -f $FLUME_HOME/lib/guava-*.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $FLUME_HOME/lib/
# ─────────────────────────────────────────────────────────────────────────

# Hadoop config pointing to our namenode
RUN mkdir -p $HADOOP_HOME/etc/hadoop
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml


ENV JAVA_HOME=/opt/java/openjdk

# Dynamic look up to verify exact location and overwrite JAVA_HOME explicitly
# RUN export REALSYS_JAVA=$(dirname $(dirname $(readlink -f $(which java)))) && \
#     echo "export JAVA_HOME=$REALSYS_JAVA" >> /opt/flume/conf/flume-env.sh

WORKDIR $FLUME_HOME

CMD ["flume-ng", "agent", \
     "--conf", "/opt/opt/flume/conf", \
     "--conf-file", "/opt/flume/conf/flume.conf", \
     "--name", "consumer", \
     "-Dflume.root.logger=INFO,console"]