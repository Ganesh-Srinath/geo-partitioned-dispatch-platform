package com.example.dispatch;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/internal")
public class InternalController {

    private final ZoneManager zones;
    private final Replicator replicator;

    public InternalController(ZoneManager zones, Replicator replicator) {
        this.zones = zones;
        this.replicator = replicator;
    }

    @GetMapping("/nearest")
    public ResponseEntity<Driver> nearest(@RequestParam("lat") double lat,
                                          @RequestParam("lng") double lng) {
        return zones.nearestFree(lat, lng)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/claim/{id}")
    public ResponseEntity<Driver> claim(@PathVariable("id") String id) {
        return zones.claimById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(409).build());
    }

    @PostMapping("/remove/{id}")
    public ResponseEntity<Driver> remove(@PathVariable("id") String id) {
        return zones.removeById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/adopt")
    public Driver adopt(@RequestBody Driver driver) {
        return zones.adopt(driver);
    }

    @PostMapping("/promote")
    public String promote() {
        replicator.promote();
        return "promoted";
    }

    @GetMapping("/nearest-many")
    public List<Driver> nearestMany(@RequestParam("lat") double lat,
                                    @RequestParam("lng") double lng,
                                    @RequestParam("k") int k) {
        return zones.nearestFree(lat, lng, k);
    }

    @GetMapping("/role")
    public String role() {
        return replicator.isFollower() ? "replica" : "primary";
    }
}