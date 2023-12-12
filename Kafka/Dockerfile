FROM confluentinc/cp-kafka:latest
COPY server.properties /etc/kafka/server.properties
COPY kafka_init.sh /kafka_init.sh
EXPOSE 9092
CMD ["/kafka_init.sh"]