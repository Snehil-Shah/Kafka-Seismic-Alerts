package org.seismic.alerts.KafkaSeismicAlerts;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloWorldController {

    @GetMapping("/hello")
    public String helloWorld() {
        return "Hello World";
    }
    @PostMapping("/report_activity")
    public String reportActivity(@RequestBody Activity a){
        System.out.println(a.name());
        return "activity recorded";
    }
}