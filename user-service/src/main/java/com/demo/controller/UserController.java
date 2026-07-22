package com.demo.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/users")
public class UserController {

    @Value("${server.port}")
    private int port;

    /**
     * GET /users/{id} - lấy user theo id
     */
    @GetMapping("/{id}")
    public String getUser(@PathVariable Long id) {
        return """
                {
                    "id": %d,
                    "name": "Nguyen Van A",
                    "instancePort": %d
                }
                """.formatted(id, port);
    }

    /**
     * GET /users - trả danh sách tất cả users (được gọi nội bộ bởi order-service)
     * instancePort giúp xác định đang được load balance tới instance nào
     */
    @GetMapping
    public List<Map<String, Object>> getAllUsers() {
        return List.of(
                Map.of("id", 1, "name", "Nguyen Van A", "email", "a@example.com", "instancePort", port),
                Map.of("id", 2, "name", "Tran Thi B",   "email", "b@example.com", "instancePort", port),
                Map.of("id", 3, "name", "Le Van C",      "email", "c@example.com", "instancePort", port)
        );
    }
}

