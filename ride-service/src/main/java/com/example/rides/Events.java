package com.example.rides;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Events {
    private Events() {}

    static String field(String json, String name) {
        Matcher m = Pattern.compile("\"" + name + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : "";
    }

    static String build(String type, String rideId, String riderId, String driverId, String detail) {
        return String.format(
                "{\"type\":\"%s\",\"rideId\":\"%s\",\"riderId\":\"%s\",\"driverId\":\"%s\",\"detail\":\"%s\"}",
                type, rideId, riderId, driverId, detail);
    }
}