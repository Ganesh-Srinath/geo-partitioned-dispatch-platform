package com.example.rides;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
public class TripTracker {

    private final Map<String, String> status = new ConcurrentHashMap<>();
    private final Map<String, List<String>> history = new ConcurrentHashMap<>();

    @KafkaListener(topics = "ride-events", groupId = "trip-tracker")
    public void on(String json) {
        String rideId = Events.field(json, "rideId");
        String type = Events.field(json, "type");
        String driverId = Events.field(json, "driverId");
        String line = driverId.isEmpty() ? type : type + " (" + driverId + ")";
        history.computeIfAbsent(rideId, k -> new CopyOnWriteArrayList<>()).add(line);
        status.put(rideId, type);
    }

    @GetMapping("/trips/{rideId}")
    public Map<String, Object> trip(@PathVariable("rideId") String rideId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("rideId", rideId);
        out.put("status", status.getOrDefault(rideId, "UNKNOWN"));
        out.put("history", history.getOrDefault(rideId, List.of()));
        return out;
    }

    @GetMapping("/trips")
    public Map<String, Long> summary() {
        Map<String, Long> counts = new LinkedHashMap<>();
        status.values().forEach(s -> counts.merge(s, 1L, Long::sum));
        return counts;
    }
}