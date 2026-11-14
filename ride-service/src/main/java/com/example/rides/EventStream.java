package com.example.rides;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
public class EventStream {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final Deque<String> recent = new ConcurrentLinkedDeque<>();

    @GetMapping("/api/events")
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        try {
            for (String json : recent) emitter.send(json);
        } catch (IOException e) {
            emitters.remove(emitter);
        }
        return emitter;
    }

    @KafkaListener(topics = "ride-events", groupId = "dashboard")
    public void on(String json) {
        recent.addLast(json);
        while (recent.size() > 30) recent.pollFirst();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(json);
            } catch (Exception e) {
                emitters.remove(emitter);
            }
        }
    }
}