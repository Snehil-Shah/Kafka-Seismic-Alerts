package kafka.consumers;

import kafka.consumers.clients.Logger;
import kafka.consumers.clients.Database;
import kafka.consumers.clients.Alert;

/**
 * Entrypoint to the Service that initializes and starts the Consumer clients
 */

public class Main {

    /**
     * Initialize all Consumer Clients in a Multi-threaded Interface
     */
    public static void main(String[] args) {
        // Instantiate Clients
        Alert emailClient = new Alert();
        Database logRegistry = new Database();
        Logger logClient = new Logger();

        // Initialize
        logRegistry.kafka_connect();
        new Thread(logClient::consume).start();
        new Thread(emailClient::enableAlerts).start();
    }
}
