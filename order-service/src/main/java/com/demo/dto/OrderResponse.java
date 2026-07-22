package com.demo.dto;

import java.util.List;

/**
 * Message gửi về gateway-service qua topic "gateway-order-replies".
 * Phải có cùng cấu trúc field với gateway-service/dto/OrderResponse.java.
 */
public record OrderResponse(
        String correlationId,
        String userId,
        List<OrderItem> orders
) {}
