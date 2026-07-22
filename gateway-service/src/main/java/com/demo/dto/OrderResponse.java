package com.demo.dto;

import java.util.List;

/**
 * Message reply từ order-service gửi về gateway qua topic "gateway-order-replies".
 *
 * correlationId: phải khớp với correlationId trong OrderRequest
 *                để gateway biết reply này thuộc request nào.
 */
public record OrderResponse(
        String correlationId,
        String userId,
        List<OrderItem> orders
) {}
