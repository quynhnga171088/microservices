package com.demo.dto;

/**
 * Message gửi từ gateway-service tới order-service qua topic "order-requests".
 *
 * correlationId: UUID dùng để ghép đôi request-reply.
 *                gateway tạo ra, order-service gửi lại trong reply.
 * userId       : lấy từ X-User-Id header (đã inject bởi JwtAuthFilter).
 * requestedBy  : email của user (dùng để log).
 */
public record OrderRequest(
        String correlationId,
        String userId,
        String requestedBy
) {}
