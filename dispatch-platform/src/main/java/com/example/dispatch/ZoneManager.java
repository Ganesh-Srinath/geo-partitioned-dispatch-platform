package com.example.dispatch;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ZoneManager {

    private static final double ZONE_SIZE = 0.1;
    private final Map<String, DriverStore> zones = new ConcurrentHashMap<>();
    private final Map<String, String> driverZone = new ConcurrentHashMap<>();

    private static long rowOf(double lat) { return (long) Math.floor(lat / ZONE_SIZE); }
    private static long colOf(double lng) { return (long) Math.floor(lng / ZONE_SIZE); }
    private static String key(long row, long col) { return row + ":" + col; }

    private DriverStore store(String key) {
        return zones.computeIfAbsent(key, k -> new DriverStore());
    }

    public Map<String, DriverStore> all() { return zones; }

    public Driver updateLocation(String id, double lat, double lng) {
        String newKey = key(rowOf(lat), colOf(lng));
        String oldKey = driverZone.put(id, newKey);
        if (oldKey != null && !oldKey.equals(newKey)) {
            boolean free = store(oldKey).remove(id).map(Driver::free).orElse(true);
            return store(newKey).put(new Driver(id, lat, lng, free));
        }
        return store(newKey).updateLocation(id, lat, lng);
    }

    public Optional<Driver> match(double lat, double lng) {
        long r = rowOf(lat), c = colOf(lng);
        while (true) {
            Driver best = null;
            String bestKey = null;
            double bestDist = Double.MAX_VALUE;
            for (long dr = -1; dr <= 1; dr++) {
                for (long dc = -1; dc <= 1; dc++) {
                    String k = key(r + dr, c + dc);
                    DriverStore s = zones.get(k);
                    if (s == null) continue;
                    Optional<Driver> cand = s.nearestFree(lat, lng);
                    if (cand.isPresent()) {
                        Driver d = cand.get();
                        double dist = Math.hypot(d.lat() - lat, d.lng() - lng);
                        if (dist < bestDist) { best = d; bestKey = k; bestDist = dist; }
                    }
                }
            }
            if (best == null) return Optional.empty();
            Optional<Driver> claimed = zones.get(bestKey).claim(best.id());
            if (claimed.isPresent()) return claimed;
        }
    }

    public Optional<Driver> nearestFree(double lat, double lng) {
        long r = rowOf(lat), c = colOf(lng);
        Driver best = null;
        double bestDist = Double.MAX_VALUE;
        for (long dr = -1; dr <= 1; dr++) {
            for (long dc = -1; dc <= 1; dc++) {
                DriverStore s = zones.get(key(r + dr, c + dc));
                if (s == null) continue;
                Optional<Driver> cand = s.nearestFree(lat, lng);
                if (cand.isPresent()) {
                    Driver d = cand.get();
                    double dist = Math.hypot(d.lat() - lat, d.lng() - lng);
                    if (dist < bestDist) { best = d; bestDist = dist; }
                }
            }
        }
        return Optional.ofNullable(best);
    }

    public Optional<Driver> claimById(String id) {
        String k = driverZone.get(id);
        return k == null ? Optional.empty() : store(k).claim(id);
    }

    public Optional<Driver> removeById(String id) {
        String k = driverZone.remove(id);
        return k == null ? Optional.empty() : store(k).remove(id);
    }

    public Driver adopt(Driver d) {
        String k = key(rowOf(d.lat()), colOf(d.lng()));
        driverZone.put(d.id(), k);
        return store(k).put(d);
    }

    public void restore(List<Driver> snapshot) {
        zones.clear();
        driverZone.clear();
        for (Driver d : snapshot) adopt(d);
    }

    public List<Driver> nearestFree(double lat, double lng, int k) {
        long r = rowOf(lat), c = colOf(lng);
        List<Driver> candidates = new ArrayList<>();
        for (long dr = -1; dr <= 1; dr++) {
            for (long dc = -1; dc <= 1; dc++) {
                DriverStore s = zones.get(key(r + dr, c + dc));
                if (s != null) candidates.addAll(s.nearestFree(lat, lng, k));
            }
        }
        candidates.sort(Comparator.comparingDouble(
                d -> Math.hypot(d.lat() - lat, d.lng() - lng)));
        return candidates.size() > k
                ? new ArrayList<>(candidates.subList(0, k)) : candidates;
    }
}