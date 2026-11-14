package com.example.dispatch;

import org.springframework.web.bind.annotation.*;
import java.util.Collection;

@RestController
@RequestMapping("/drivers")
public class DriverController {

    private final ZoneManager zones;

    public DriverController(ZoneManager zones) { this.zones = zones; }

    @PostMapping("/location")
    public Driver update(@RequestBody Driver driver) {
        return zones.updateLocation(driver.id(), driver.lat(), driver.lng());
    }

    @GetMapping
    public Collection<Driver> all() {
        return zones.all().values().stream()
                .flatMap(s -> s.all().stream()).toList();
    }
}