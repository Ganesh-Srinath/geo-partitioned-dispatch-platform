package com.example.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@RestController
public class GatewayController {

    static class Shard {
        final String name;
        final List<String> servers;
        private int active = 0;

        Shard(String name, String primary, String replica) {
            this.name = name;
            this.servers = List.of(primary, replica);
        }

        synchronized String url() { return servers.get(active); }

        synchronized boolean switchFrom(String failedUrl) {
            if (!servers.get(active).equals(failedUrl)) return true; // already switched
            if (active == servers.size() - 1) return false;          // no standby left
            active++;
            return true;
        }
    }

    private static final Shard SOUTH = new Shard("south", "http://localhost:8081", "http://localhost:8083");
    private static final Shard NORTH = new Shard("north", "http://localhost:8082", "http://localhost:8084");
    private static final List<Shard> SHARDS = List.of(SOUTH, NORTH);
    private static final double SPLIT_LAT = 13.0;
    private static final int MAX_ATTEMPTS = 5;
    private static final String TOPIC = "ride-events";

    // How many of the nearest free drivers a rider chooses from.
    // 1 behaves like the old "always the nearest"; 5 is the hot spot fix.
    @Value("${dispatch.top-k:5}")
    private int topK;

    record Driver(String id, double lat, double lng, boolean free) {}
    record RideRequest(String riderId, String rideId, double lat, double lng) {}
    record Candidate(Driver driver, Shard shard, double dist) {}

    private final RestClient http = RestClient.create();
    private final KafkaTemplate<String, String> kafka;
    private final Map<String, Shard> driverShard = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<ResponseEntity<Object>>> rides = new ConcurrentHashMap<>();
    private final Map<String, String[]> driverRide = new ConcurrentHashMap<>();

    public GatewayController(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    private Shard shardFor(double lat) {
        return lat < SPLIT_LAT ? SOUTH : NORTH;
    }

    // Run an action on the shard's active server. If it cannot be reached,
    // switch to the standby, promote it, and retry once.
    private <T> T call(Shard shard, Function<String, T> action) {
        String url = shard.url();
        try {
            return action.apply(url);
        } catch (ResourceAccessException e) {
            if (!shard.switchFrom(url)) throw e;
            try {
                http.post().uri(shard.url() + "/internal/promote").retrieve().toBodilessEntity();
            } catch (RestClientException ignored) {
                // best effort
            }
            return action.apply(shard.url());
        }
    }

    @PostMapping("/drivers/location")
    public ResponseEntity<Object> location(@RequestBody Driver ping) {
        Shard newShard = shardFor(ping.lat());
        Shard oldShard = driverShard.get(ping.id());
        ResponseEntity<Object> res;
        if (oldShard != null && oldShard != newShard) {
            Driver old;
            try {
                old = removeOn(oldShard, ping.id());
            } catch (ResourceAccessException e) {
                return ResponseEntity.status(503).body("Old zone server unreachable, try again");
            }
            boolean free = (old == null) || old.free();
            res = forward(newShard, "/internal/adopt",
                    new Driver(ping.id(), ping.lat(), ping.lng(), free));
        } else {
            res = forward(newShard, "/drivers/location", ping);
        }
        if (res.getStatusCode().is2xxSuccessful()) driverShard.put(ping.id(), newShard);
        return res;
    }

    @PostMapping("/rides/request")
    public ResponseEntity<Object> request(@RequestBody RideRequest req) {
        String rideId = (req.rideId() == null || req.rideId().isBlank())
                ? UUID.randomUUID().toString() : req.rideId();
        String riderId = req.riderId() == null ? "" : req.riderId();

        // Idempotency: the first request with this rideId does the work,
        // any repeat gets the same answer back.
        CompletableFuture<ResponseEntity<Object>> mine = new CompletableFuture<>();
        CompletableFuture<ResponseEntity<Object>> existing = rides.putIfAbsent(rideId, mine);
        if (existing != null) return existing.join();

        publish("RideRequested", rideId, riderId, "", "");
        ResponseEntity<Object> res;
        try {
            res = match(req, rideId, riderId);
        } catch (RuntimeException e) {
            rides.remove(rideId, mine);
            mine.completeExceptionally(e);
            throw e;
        }
        if (!res.getStatusCode().is2xxSuccessful()) rides.remove(rideId, mine); // failed rides may be retried
        mine.complete(res);
        return res;
    }

    private ResponseEntity<Object> match(RideRequest req, String rideId, String riderId) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            List<Candidate> candidates = new ArrayList<>();
            for (Shard shard : SHARDS) {
                for (Driver d : nearestManyOn(shard, req.lat(), req.lng(), topK)) {
                    double dist = Math.hypot(d.lat() - req.lat(), d.lng() - req.lng());
                    candidates.add(new Candidate(d, shard, dist));
                }
            }
            if (candidates.isEmpty()) {
                publish("RideFailed", rideId, riderId, "", "no free driver");
                return ResponseEntity.status(404).body("No free driver");
            }
            candidates.sort(Comparator.comparingDouble(Candidate::dist));
            List<Candidate> top = new ArrayList<>(
                    candidates.subList(0, Math.min(topK, candidates.size())));
            Collections.shuffle(top); // spread the crowd across the nearest few drivers
            for (Candidate c : top) {
                Driver claimed = claimOn(c.shard(), c.driver().id());
                if (claimed != null) {
                    driverRide.put(claimed.id(), new String[] {rideId, riderId});
                    publish("DriverMatched", rideId, riderId, claimed.id(), "");
                    return ResponseEntity.ok(view(rideId, claimed));
                }
            }
            // every candidate was taken: look again
        }
        publish("RideFailed", rideId, riderId, "", "too busy");
        return ResponseEntity.status(503).body("Too busy, try again");
    }

    @PostMapping("/rides/complete/{driverId}")
    public ResponseEntity<Object> complete(@PathVariable("driverId") String driverId) {
        ResponseEntity<Object> res = release(driverId);
        if (res.getStatusCode().is2xxSuccessful()) {
            String[] ride = driverRide.remove(driverId);
            if (ride != null) publish("RideCompleted", ride[0], ride[1], driverId, "");
        }
        return res;
    }

    // Used by the saga compensator: frees the driver without announcing a completed ride.
    @PostMapping("/rides/release/{driverId}")
    public ResponseEntity<Object> releaseOnly(@PathVariable("driverId") String driverId) {
        driverRide.remove(driverId);
        return release(driverId);
    }

    private ResponseEntity<Object> release(String driverId) {
        for (Shard shard : SHARDS) {
            ResponseEntity<Object> res = forward(shard, "/rides/complete/" + driverId, null);
            if (res.getStatusCode().is2xxSuccessful()) return res;
        }
        return ResponseEntity.status(404).body("Unknown driver");
    }

    @GetMapping("/drivers")
    public List<Object> all() {
        List<Object> out = new ArrayList<>();
        for (Shard shard : SHARDS) {
            try {
                List<?> part = call(shard, url ->
                        http.get().uri(url + "/drivers").retrieve().body(List.class));
                if (part != null) out.addAll(part);
            } catch (ResourceAccessException e) {
                // both servers of this shard are down, skip it
            }
        }
        return out;
    }

    private List<Driver> nearestManyOn(Shard shard, double lat, double lng, int k) {
        try {
            Driver[] found = call(shard, url -> http.get()
                    .uri(url + "/internal/nearest-many?lat={lat}&lng={lng}&k={k}", lat, lng, k)
                    .retrieve().body(Driver[].class));
            return found == null ? List.of() : List.of(found);
        } catch (RestClientResponseException | ResourceAccessException e) {
            return List.of();
        }
    }

    private Driver claimOn(Shard shard, String id) {
        try {
            return call(shard, url -> http.post()
                    .uri(url + "/internal/claim/{id}", id)
                    .retrieve().body(Driver.class));
        } catch (RestClientResponseException | ResourceAccessException e) {
            return null; // 409 means someone else claimed first
        }
    }

    private Driver removeOn(Shard shard, String id) {
        try {
            return call(shard, url -> http.post()
                    .uri(url + "/internal/remove/{id}", id)
                    .retrieve().body(Driver.class));
        } catch (RestClientResponseException e) {
            return null; // the driver was not there
        }
    }

    private ResponseEntity<Object> forward(Shard shard, String path, Object body) {
        try {
            return call(shard, url -> {
                RestClient.RequestBodySpec spec = http.post().uri(url + path)
                        .contentType(MediaType.APPLICATION_JSON);
                if (body != null) spec = spec.body(body);
                return ResponseEntity.ok(spec.retrieve().body(Object.class));
            });
        } catch (RestClientResponseException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (ResourceAccessException e) {
            return ResponseEntity.status(503).body("Zone server unreachable");
        }
    }

    private static Map<String, Object> view(String rideId, Driver d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("rideId", rideId);
        m.put("id", d.id());
        m.put("lat", d.lat());
        m.put("lng", d.lng());
        m.put("free", d.free());
        return m;
    }

    private void publish(String type, String rideId, String riderId, String driverId, String detail) {
        String json = String.format(
                "{\"type\":\"%s\",\"rideId\":\"%s\",\"riderId\":\"%s\",\"driverId\":\"%s\",\"detail\":\"%s\"}",
                type, esc(rideId), esc(riderId), esc(driverId), esc(detail));
        try {
            kafka.send(TOPIC, rideId, json).whenComplete((r, ex) -> {
                if (ex != null) System.out.println("Could not publish " + type + ": " + ex.getMessage());
            });
        } catch (RuntimeException e) {
            System.out.println("Could not publish " + type + ": " + e.getMessage());
        }
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("\"", "").replace("\\", "");
    }
}