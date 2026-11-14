package com.example.dispatch;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.Optional;

record RideRequest(String riderId, double lat, double lng) {}

@RestController
@RequestMapping("/rides")
public class RideController {

    private final ZoneManager zones;

    public RideController(ZoneManager zones) { this.zones = zones; }

    @PostMapping("/request")
    public Driver request(@RequestBody RideRequest req) {
        return zones.match(req.lat(), req.lng())
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "No free driver"));
    }

    @PostMapping("/complete/{driverId}")
    public Driver complete(@PathVariable String driverId) {
        return zones.all().values().stream()
                .map(s -> s.release(driverId))
                .flatMap(Optional::stream)
                .findFirst()
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Unknown driver"));
    }
}