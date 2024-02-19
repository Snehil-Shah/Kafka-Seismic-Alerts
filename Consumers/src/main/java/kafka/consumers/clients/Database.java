package kafka.consumers.clients;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Datastore Consumer Class:
 * Initializes Kafka-JDBC Connection
 */

public class Database {

    /**
     * Initializes Database Connection to Kafka Stream as a Sink
     */
    public void kafka_connect() {
        try {
            URI connector_uri = new URI("http://kafka-db-connector:8083/connectors"); // API
            String kafka_connector_config = "{\n" +
                    "    \"name\": \"Logs-Sink\",\n" +
                    "    \"config\": {\n" +
                    "      \"connector.class\": \"io.confluent.connect.jdbc.JdbcSinkConnector\",\n" +
                    "      \"tasks.max\": \"1\",\n" +
                    "      \"topics\": \"severe_seismic_events,minor_seismic_events\",\n" +
                    "      \"connection.url\": \"" + System.getenv("KAFKA_JDBC_CONNECTION_URL") + "\",\n" +
                    "      \"connection.user\": \"" + System.getenv("DB_USERNAME") + "\",\n" +
                    "      \"connection.password\": \"" + System.getenv("DB_PASSWORD") + "\",\n" +
                    "      \"auto.create\": \"true\"\n" +
                    "    }\n" +
                    "}";

            while (true) {
                try {
                    HttpURLConnection connection = (HttpURLConnection) connector_uri.toURL().openConnection();

                    connection.setDoOutput(true);
                    connection.setRequestMethod("POST");
                    connection.setRequestProperty("Content-Type", "application/json");

                    // Send Kafka-JDBC Connector Config in POST Request
                    try (OutputStream request_stream = connection.getOutputStream()) {
                        byte[] body = kafka_connector_config.getBytes("utf-8");
                        request_stream.write(body, 0, body.length);
                    }
                    if ("Created".equals(connection.getResponseMessage())) {
                        // Kafka-DB Connection Successful
                        System.out.println("-> Database initialized..");
                        connection.disconnect();
                        break;
                    } else {
                        connection.disconnect();
                        Thread.sleep(5000);
                    }
                } catch (Exception e) {
                    Thread.sleep(5000);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
