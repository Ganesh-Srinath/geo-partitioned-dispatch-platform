package com.example.dispatch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import java.util.List;

@Component
public class Replicator {

    private final ZoneManager zones;
    private final RestClient http = RestClient.create();
    private final String primaryUrl;
    private volatile boolean follower;

    public Replicator(ZoneManager zones, @Value("${replica.of:}") String primaryUrl) {
        this.zones = zones;
        this.primaryUrl = primaryUrl;
        this.follower = !primaryUrl.isBlank();
    }

    @Scheduled(fixedDelay = 1000)
    public synchronized void pull() {
        if (!follower) return;
        try {
            List<Driver> snapshot = http.get().uri(primaryUrl + "/drivers")
                    .retrieve().body(new ParameterizedTypeReference<List<Driver>>() {});
            if (snapshot != null) zones.restore(snapshot);
        } catch (RestClientException e) {
            // primary unreachable: keep existing copy
        }
    }

    public synchronized void promote() { follower = false; }

    public boolean isFollower() { return follower; }
}