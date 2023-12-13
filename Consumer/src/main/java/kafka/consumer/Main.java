package kafka.consumer;
import kafka.consumer.clients.Logger;

public class Main {
    public static void main(String[] args) {
        Logger logClient = new Logger();
        logClient.consume();
    }
}
