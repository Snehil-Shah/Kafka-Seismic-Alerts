package kafka.consumers;

import kafka.consumers.clients.Logger;
import kafka.consumers.clients.Database;

/**
 * Entrypoint to the Service that initializes and starts both Consumer clients
 */

public class Main {
    public static void main(String[] args) {
        System.out.println("Waiting for Kafka Broker & Database Sink Connector..");
        Database logRegistry = new Database();
        logRegistry.kafka_connect();
        Logger logClient = new Logger();
        logClient.consume();
    }
}
