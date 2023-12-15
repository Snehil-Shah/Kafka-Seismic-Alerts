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
        String RESET = "\033[0m";
        String PURPLE = "\033[35m";
        System.out.println(PURPLE+"Waiting for Kafka Broker & Database Sink Connector.."+RESET);
        Database logRegistry = new Database();
        Logger logClient = new Logger();
        logRegistry.kafka_connect();
        System.out.println("-> Alerts Enabled..");
        new Thread(logClient::consume).start();
        new Thread(emailClient::enableAlerts).start();

    }
}
