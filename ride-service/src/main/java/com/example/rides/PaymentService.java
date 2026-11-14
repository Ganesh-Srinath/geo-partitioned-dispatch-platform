package com.example.rides;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PaymentService {

    private final KafkaTemplate<String, String> kafka;
    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    public PaymentService(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = "ride-events", groupId = "payment-service")
    public void on(String json) {
        if (!Events.field(json, "type").equals("DriverMatched")) return;
        String rideId = Events.field(json, "rideId");
        String riderId = Events.field(json, "riderId");
        String driverId = Events.field(json, "driverId");
        if (!seen.add(rideId)) return; // duplicate delivery: already handled

        boolean ok = !riderId.startsWith("nopay");
        kafka.send("ride-events", rideId, Events.build(
                ok ? "PaymentAuthorized" : "PaymentFailed",
                rideId, riderId, driverId, ok ? "" : "card declined"));
    }
}