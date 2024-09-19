FROM openjdk:8-jre-slim
# ARG kafka_version
# ARG kafka_package
# WORKDIR /app/create-topics.sh

ENV KAFKA_HOME=/app/3.7.0 PATH=$PATH:.

COPY .create-topics.sh /usr/local/bin/create-topics.sh

RUN apt update \
    && apt install -y bash \
    && apt install -y wget \
    && wget https://archive.apache.org/dist/kafka/3.7.0/kafka-3.7.0-src.tgz \
    && tar xzf kafka-3.7.0-src.tgz \
    && chmod +x /usr/local/bin/create-topics.sh \
    && rm -rf /var/cache/apk/* 


ENTRYPOINT ["/usr/local/bin/create-topics.sh"]