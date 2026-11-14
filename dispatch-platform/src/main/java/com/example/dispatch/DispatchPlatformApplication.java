package com.example.dispatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DispatchPlatformApplication {
    public static void main(String[] args) {
        SpringApplication.run(DispatchPlatformApplication.class, args);
    }
}