package com.example.rides;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Compensator {

    private final RestClient http = RestClient.create();
    private final KafkaTemplate<String, String> kafka;
    private final String gatewayUrl;
    private final Set<String> done = ConcurrentHashMap.newKeySet();

    public Compensator(KafkaTemplate<String, String> kafka,
                       @Value("${gateway.url}") String gatewayUrl) {
        this.kafka = kafka;
        this.gatewayUrl = gatewayUrl;
    }

    @KafkaListener(topics = "ride-events", groupId = "saga-compensator")
    public void on(String json) {
        if (!Events.field(json, "type").equals("PaymentFailed")) return;
        String rideId = Events.field(json, "rideId");
        String riderId = Events.field(json, "riderId");
        String driverId = Events.field(json, "driverId");
        if (done.contains(rideId)) return; // already compensated

        http.post().uri(gatewayUrl + "/rides/release/{id}", driverId)
                .retrieve().toBodilessEntity();
        kafka.send("ride-events", rideId, Events.build(
                "RideCancelled", rideId, riderId, driverId,
                "payment failed, driver released"));
        done.add(rideId);
    }
}