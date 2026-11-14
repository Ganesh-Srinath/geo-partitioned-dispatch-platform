package com.example.dispatch;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DriverStore {
    private final Map<String, Driver> drivers = new ConcurrentHashMap<>();

    public void save(Driver driver) { drivers.put(driver.id(), driver); }

    public Collection<Driver> all() { return drivers.values(); }

    public Optional<Driver> nearestFree(double lat, double lng) {
        return drivers.values().stream()
                .filter(Driver::free)
                .min(Comparator.comparingDouble(
                        d -> Math.hypot(d.lat() - lat, d.lng() - lng)));
    }

    public Optional<Driver> claim(String id) {
        boolean[] won = {false};
        Driver result = drivers.computeIfPresent(id, (k, d) -> {
            if (d.free()) {
                won[0] = true;
                return new Driver(d.id(), d.lat(), d.lng(), false);
            }
            return d;
        });
        return won[0] ? Optional.of(result) : Optional.empty();
    }

    public Optional<Driver> release(String id) {
        boolean[] found = {false};
        Driver result = drivers.computeIfPresent(id, (k, d) -> {
            found[0] = true;
            return new Driver(d.id(), d.lat(), d.lng(), true);
        });
        return found[0] ? Optional.of(result) : Optional.empty();
    }

    public Optional<Driver> remove(String id) {
        return Optional.ofNullable(drivers.remove(id));
    }

    public Driver put(Driver d) {
        drivers.put(d.id(), d);
        return d;
    }

    public Driver updateLocation(String id, double lat, double lng) {
        return drivers.compute(id, (k, old) ->
                new Driver(id, lat, lng, old == null || old.free()));
    }

    private record Scored(Driver driver, double dist) {}

    public List<Driver> nearestFree(double lat, double lng, int k) {
        // max-heap: the top is the farthest of the k closest found so far
        PriorityQueue<Scored> heap = new PriorityQueue<>(
                Comparator.comparingDouble(Scored::dist).reversed());
        for (Driver d : drivers.values()) {
            if (!d.free()) continue;
            double dx = d.lat() - lat;
            double dy = d.lng() - lng;
            double dist = dx * dx + dy * dy;
            if (heap.size() < k) {
                heap.add(new Scored(d, dist));
            } else if (dist < heap.peek().dist()) {
                heap.poll();
                heap.add(new Scored(d, dist));
            }
        }
        List<Scored> best = new ArrayList<>(heap);
        best.sort(Comparator.comparingDouble(Scored::dist));
        List<Driver> out = new ArrayList<>();
        for (Scored s : best) out.add(s.driver());
        return out;
    }
}