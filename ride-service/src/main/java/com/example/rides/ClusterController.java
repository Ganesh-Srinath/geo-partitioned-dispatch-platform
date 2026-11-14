package com.example.rides;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
public class ClusterController {

    private static final Map<String, String> SERVERS = new LinkedHashMap<>();
    static {
        SERVERS.put("south-primary", "http://localhost:8081");
        SERVERS.put("south-replica", "http://localhost:8083");
        SERVERS.put("north-primary", "http://localhost:8082");
        SERVERS.put("north-replica", "http://localhost:8084");
    }

    private final RestClient http;
    private final String gatewayUrl;

    public ClusterController(@Value("${gateway.url}") String gatewayUrl) {
        this.gatewayUrl = gatewayUrl;
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build());
        factory.setReadTimeout(Duration.ofMillis(1500));
        this.http = RestClient.builder().requestFactory(factory).build();
    }

    @GetMapping("/api/cluster")
    public Map<String, Object> cluster() {
        List<Map<String, Object>> servers = new ArrayList<>();
        SERVERS.forEach((name, url) -> {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("name", name);
            try {
                String role = http.get().uri(url + "/internal/role")
                        .retrieve().body(String.class);
                s.put("up", true);
                s.put("role", role);
            } catch (RestClientException e) {
                s.put("up", false);
                s.put("role", "down");
            }
            servers.add(s);
        });

        boolean gatewayUp = true;
        List<?> drivers = List.of();
        try {
            List<?> found = http.get().uri(gatewayUrl + "/drivers")
                    .retrieve().body(List.class);
            if (found != null) drivers = found;
        } catch (RestClientException e) {
            gatewayUp = false;
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("servers", servers);
        out.put("gatewayUp", gatewayUp);
        out.put("drivers", drivers);
        return out;
    }
}