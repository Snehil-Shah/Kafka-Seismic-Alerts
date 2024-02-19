package kafka.consumers.clients;

import java.util.Properties;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.*;
import java.util.Scanner;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.consumers.ConsumerConfig;

/**
 * Email Alerts Consumer Class
 * Initializes SMTP Client to enable Email Alerts
 */

public class Alert {
    private ConsumerConfig emailClientConfig = new ConsumerConfig("emailClient");
    private KafkaConsumer<String, String> emailClient = new KafkaConsumer<>(emailClientConfig.getProperties());
    private final String service_email = System.getenv("SERVICE_EMAIL_ID");
    private final String password = System.getenv("SERVICE_EMAIL_PASSWORD");
    private Session session;
    private Properties config;
    private String name;
    private String email;

    /**
     * Constructor to Configure & initialize SMTP Client
     */
    public Alert() {
        // SMTP Configuration
        config = new Properties();
        config.put("mail.smtp.host", "smtp.gmail.com");
        config.put("mail.smtp.port", "587");
        config.put("mail.smtp.auth", "true");
        config.put("mail.smtp.starttls.enable", "true");
        session = Session.getInstance(config,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(service_email, password);
                    }
                });

        // Take Recipient Details
        String RESET = "\033[0m";
        String PURPLE = "\033[35m";
        String BOLD = "\033[1m";

        System.out.println(BOLD + "\nWelcome to Seismic Logger & Alerts Client!\n" + RESET);
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter your name: ");
        name = scanner.nextLine();

        System.out.print("Enter your email: ");
        email = scanner.nextLine();

        System.out.print("Registered for Email Alerts!\n\n");
        scanner.close();
        System.out.println(PURPLE + "Waiting for Kafka Broker & Database Sink Connector.." + RESET);
        System.out.println("-> Alerts Enabled..");
    }

    /**
     * Send Seismic Activity Alert Email to the Registered Recipient
     *
     * @param user_name      Recipient's Name
     * @param recipient_mail Recipient's Email
     * @param time           Time of Seismic Activity
     * @param region         Region of Seismic Activity
     * @param co_ordinates   Co-ordinates of Seismic Activity
     * @param magnitude      Magnitude of Seismic Activity
     */
    private void send_mail(String user_name, String recipient_mail, String time, String region, String co_ordinates,
            float magnitude) {
        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(service_email));
            message.setRecipients(
                    Message.RecipientType.TO,
                    InternetAddress.parse(recipient_mail));
            message.setSubject("ALERT: Unusual Seismic Activity in " + region + " Detected!");
            message.setContent("<center><h2>SEISMIC ALERTS</h2></center>" + "<p>Dear " + user_name + "!</p>" +
                    "<p>We are reaching out to inform you about an unusual seismic activity detected in " + region +
                    " of magnitude " + magnitude + " on " + time +
                    ".<br>We want to ensure that you are aware of the situation and take necessary precautions to ensure your safety.</p>"
                    +
                    "<h2>Details:</h2>" +
                    "<ul>" +
                    "<li><strong>Region:</strong> " + region + "</li>" +
                    "<li><strong>Magnitude:</strong> " + magnitude + "</li>" +
                    "<li><strong>Coordinates:</strong> " + co_ordinates + "</li>" +
                    "<li><strong>Time:</strong> " + time + "</li>" +
                    "</ul>" +
                    "<h2>Safety Precautions:</h2>" +
                    "<ol>" +
                    "<li><strong>Drop, Cover, and Hold On:</strong> In the event of an earthquake, remember to drop to the ground, take cover under a sturdy piece of furniture if possible, and hold on until the shaking stops.</li>"
                    +
                    "<li><strong>Evacuation Plan:</strong> Familiarize yourself with the evacuation routes in your area and have a designated meeting point for your family or household.</li>"
                    +
                    "<li><strong>Emergency Kit:</strong> Ensure you have a well-stocked emergency kit with essential supplies, including water, non-perishable food, first aid supplies, flashlight, and any necessary medications.</li>"
                    +
                    "<li><strong>Stay Informed:</strong> Keep yourself updated with the latest information from local authorities and follow their instructions promptly.</li>"
                    +
                    "</ol>" +
                    "<h2>Additional Resources:</h2>" +
                    "<ol>" +
                    "<li><a href='https://www.iccsafe.org/advocacy/safety-toolkits/earthquake-safety-and-resources/'>International Code Council - Safety & Resources</a></li>"
                    +
                    "<li>Use our <a href='https://github.com/Snehil-Shah/Seismic-Alerts-Streamer'>Seismic Logger & Alerts Consumer Clients</a> to receive live updates and alerts!</li>"
                    +
                    "<li>Use our endpoint <a href='http://localhost:5000/seismic_events'>localhost:5000/events</a> to POST Seismic Activity Updates in your Region to Help the Community or GET Seismic Activity Log Archives for Research Purposes!</li>"
                    +
                    "</ol>" +
                    "<br><p>Stay Safe!<br><i>Snehil Shah</i></p>", "text/html");
            Transport.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Subscribe to Kafka Topic and Enable Alerts Service
     */
    public void enableAlerts() {
        emailClient.subscribe((Arrays.asList("severe_seismic_events")));

        while (true) {
            ObjectMapper mapper = new ObjectMapper();
            ConsumerRecords<String, String> records = emailClient.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Map<String, Map<String, Object>> recordVal = mapper.readValue(record.value(),
                            new TypeReference<Map<String, Map<String, Object>>>() {});
                    send_mail(name, email,
                            recordVal.get("payload").get("time").toString().replaceAll("T", " at ").replaceAll("Z",
                                    "-UTC"),
                            recordVal.get("payload").get("region").toString(),
                            recordVal.get("payload").get("co_ordinates").toString(),
                            Float.parseFloat(recordVal.get("payload").get("magnitude").toString()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
