package com.demo.dto;

/**
 * Message nhận từ gateway-service qua topic "order-requests".
 * Phải có cùng cấu trúc field với gateway-service/dto/OrderRequest.java.
 */
public record OrderRequest(
        String correlationId,
        String userId,
        String requestedBy
) {}
