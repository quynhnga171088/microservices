package com.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

@RestController
@RequestMapping("/users")
public class OrderController {

    private final RestClient userServiceClient;

    public OrderController(RestClient.Builder loadBalancedRestClientBuilder) {
        // "http://user-service" → Consul sẽ resolve sang địa chỉ thực của user-service
        // @LoadBalanced sẽ tự động load balance nếu có nhiều instance
        this.userServiceClient = loadBalancedRestClientBuilder
                .baseUrl("http://user-service")
                .build();
    }

    /**
     * GET /users/list-all
     * Gateway route: Path=/users/list-all → lb://order-service
     *
     * order-service gọi nội bộ:
     *   GET http://user-service/users → Consul lookup → user-service thực tế
     */
    @GetMapping("/list-all")
    public String listAllUsers() {
        return userServiceClient.get()
                .uri("/users")
                .retrieve()
                .body(String.class);
    }
}
