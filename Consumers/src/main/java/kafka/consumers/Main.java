package kafka.consumers;

import kafka.consumers.clients.Logger;
import kafka.consumers.clients.Database;
import kafka.consumers.clients.Alert;

/**
 * Entrypoint to the Service that initializes and starts both Consumer clients
 */

public class Main {
    public static void main(String[] args) {
        Alert emailClient = new Alert();
        System.out.println("Waiting for Kafka Broker & Database Sink Connector..");
        Database logRegistry = new Database();
        Logger logClient = new Logger();
        logRegistry.kafka_connect();
        new Thread(logClient::consume).start();
        new Thread(emailClient::enableAlerts).start();
    }
}
