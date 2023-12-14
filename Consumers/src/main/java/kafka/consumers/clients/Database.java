package kafka.consumers.clients;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

public class Database {
    public void kafka_connect() {
        try {
            URI connector_uri = new URI("http://kafka-db-connector:8083/connectors");
            HttpURLConnection connection = (HttpURLConnection) connector_uri.toURL().openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            String kafka_connector_config = "{\n" +
                    "    \"name\": \"Logs-Sink\",\n" +
                    "    \"config\": {\n" +
                    "      \"connector.class\": \"io.confluent.connect.jdbc.JdbcSinkConnector\",\n" +
                    "      \"tasks.max\": \"1\",\n" +
                    "      \"topics\": \"severe_seismic_events,minor_seismic_events\",\n" +
                    "      \"connection.url\": \"jdbc:postgresql://db:5432/Logs\",\n" +
                    "      \"connection.user\": \"postgres\",\n" +
                    "      \"connection.password\": \"password\",\n" +
                    "      \"auto.create\": \"true\"\n" +
                    "    }\n" +
                    "}";
            while (true) {
                try {
                    try (OutputStream request_stream = connection.getOutputStream()) {
                        byte[] body = kafka_connector_config.getBytes("utf-8");
                        request_stream.write(body, 0, body.length);
                    }
                    if ("Created".equals(connection.getResponseMessage())) {
                        System.out.println("Database initialized and connected..");
                        connection.disconnect();
                        break;
                    }
                    else{
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
