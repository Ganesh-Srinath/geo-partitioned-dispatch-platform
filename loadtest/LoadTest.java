import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoadTest {

    static final String BASE = "http://localhost:8080";
    static final HttpClient HTTP = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .build();
    static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");
    static final Pattern OBJECT = Pattern.compile("\\{[^{}]*\\}");

    record Result(int status, String driverId, long nanos) {}

    static HttpResponse<String> post(String path, String json) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    }

    static String get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).GET().build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }

    static long percentile(List<Long> sorted, double p) {
        int i = Math.min(sorted.size() - 1, (int) (sorted.size() * p));
        return sorted.get(i);
    }

    public static void main(String[] args) throws Exception {
        int drivers = args.length > 0 ? Integer.parseInt(args[0]) : 200;
        int riders = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : 100;
        boolean hotspot = args.length > 3 && args[3].equals("hotspot");
        Random rnd = new Random(42);

        // 1. Register drivers and ensure initial state is clean/free
        for (int i = 0; i < drivers; i++) {
            double lat = 12.80 + rnd.nextDouble() * 0.40;
            double lng = 77.50 + rnd.nextDouble() * 0.20;
            post("/drivers/location", String.format(Locale.US,
                "{\"id\":\"load-%d\",\"lat\":%.5f,\"lng\":%.5f,\"free\":true}", i, lat, lng));
            post("/rides/complete/load-" + i, "");
        }

        // 2. Prepare concurrent ride request tasks
        List<Callable<Result>> tasks = new ArrayList<>();
        for (int i = 0; i < riders; i++) {
            double lat = hotspot ? 13.00 : 12.80 + rnd.nextDouble() * 0.40;
            double lng = hotspot ? 77.60 : 77.50 + rnd.nextDouble() * 0.20;
            String body = String.format(Locale.US,
                "{\"riderId\":\"r%d\",\"lat\":%.5f,\"lng\":%.5f}", i, lat, lng);
            tasks.add(() -> {
                long t0 = System.nanoTime();
                try {
                    HttpResponse<String> res = post("/rides/request", body);
                    String id = null;
                    if (res.statusCode() == 200) {
                        Matcher m = ID.matcher(res.body());
                        if (m.find()) id = m.group(1);
                    }
                    return new Result(res.statusCode(), id, System.nanoTime() - t0);
                } catch (Exception e) {
                    return new Result(-1, null, System.nanoTime() - t0);
                }
            });
        }

        // 3. Execute concurrently via thread pool
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        long start = System.nanoTime();
        List<Future<Result>> futures = pool.invokeAll(tasks);
        double seconds = (System.nanoTime() - start) / 1e9;
        pool.shutdown();

        // 4. Aggregate metrics
        Map<Integer, Integer> byStatus = new TreeMap<>();
        Map<String, Integer> assigned = new HashMap<>();
        List<Long> latencies = new ArrayList<>();
        int successes = 0;
        for (Future<Result> f : futures) {
            Result r = f.get();
            byStatus.merge(r.status(), 1, Integer::sum);
            latencies.add(r.nanos());
            if (r.driverId() != null) {
                assigned.merge(r.driverId(), 1, Integer::sum);
                successes++;
            }
        }
        Collections.sort(latencies);
        long doubleBooked = assigned.values().stream().filter(c -> c > 1).count();

        // 5. Query cluster for actual busy count
        int busyInSystem = 0;
        Matcher objects = OBJECT.matcher(get("/drivers"));
        while (objects.find()) {
            String o = objects.group();
            if (o.contains("\"id\":\"load-") && o.contains("\"free\":false")) busyInSystem++;
        }

        // 6. Output benchmark report
        System.out.println();
        System.out.println("Drivers registered:       " + drivers);
        System.out.println("Ride requests sent:       " + riders + " (" + threads + " threads"
            + (hotspot ? ", all at one spot" : "") + ")");
        System.out.printf(Locale.US, "Total time:               %.2f s%n", seconds);
        System.out.printf(Locale.US, "Throughput:               %.0f requests/second%n", riders / seconds);
        System.out.printf(Locale.US, "Latency p50/p95/p99/max:  %.1f / %.1f / %.1f / %.1f ms%n",
            percentile(latencies, 0.50) / 1e6, percentile(latencies, 0.95) / 1e6,
            percentile(latencies, 0.99) / 1e6, latencies.get(latencies.size() - 1) / 1e6);
        System.out.println("Responses by status:      " + byStatus
            + "   (200 = matched, 404 = no free driver, 503 = too busy, -1 = connection error)");
        System.out.println("Riders matched:           " + successes);
        System.out.println("Distinct drivers matched: " + assigned.size());
        System.out.println("Double bookings:          " + doubleBooked);
        System.out.println("Busy drivers in system:   " + busyInSystem);
        boolean pass = doubleBooked == 0 && busyInSystem == successes;
        System.out.println("RESULT: " + (pass ? "PASS" : "FAIL"));
    }
}