FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.5.10
ENV HBASE_HOME=/opt/hbase
ENV PATH=$HBASE_HOME/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends curl procps \
 && curl -fsSL https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    | tar -xz -C /opt \
 && mv /opt/hbase-${HBASE_VERSION} $HBASE_HOME \
 && rm -rf /var/lib/apt/lists/*

COPY hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

# 16010 = Master web UI, 9090 = Thrift (Python/happybase), 8080 = REST, 2181 = ZooKeeper
EXPOSE 16010 9090 8080 2181 16000 16020

CMD bash -c "start-hbase.sh && hbase-daemon.sh start thrift && hbase-daemon.sh start rest && tail -F $HBASE_HOME/logs/*"
