package com.demo.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/parts")
public class PartsController {
    @Value("${server.port}")
    private int port;

    @GetMapping("/{id}")
    public String getUser(@PathVariable Long id) {
        return """
                {
                    "id": %d,
                    "name": "Administration Department",
                    "instancePort": %d
                }
                """.formatted(id, port);
    }
    
    @GetMapping
    public List<Map<String, Object>> getAllParts() {
        return List.of(
                Map.of("id", 1, "name", "Administration Department", "instancePort", port),
                Map.of("id", 2, "name", "Human Resources Department", "instancePort", port),
                Map.of("id", 3, "name", "Finance Department", "instancePort", port)
        );
    }
}
