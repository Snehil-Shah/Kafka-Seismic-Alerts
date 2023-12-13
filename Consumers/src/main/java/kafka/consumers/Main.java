package kafka.consumers;
import kafka.consumers.clients.Logger;

/**
 * Entrypoint to the Service that initializes and starts both Consumer clients
 */

public class Main {
    public static void main(String[] args) {
        System.out.println("hello world");
        Logger logClient = new Logger();
        logClient.consume();
    }
}
