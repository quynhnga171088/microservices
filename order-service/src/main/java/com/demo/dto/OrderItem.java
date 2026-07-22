package com.demo.dto;

/**
 * Một đơn hàng trong danh sách phản hồi.
 * Phải có cùng cấu trúc field với gateway-service/dto/OrderItem.java.
 */
public record OrderItem(
        String orderId,
        String productName,
        int quantity,
        double totalPrice,
        String status
) {}
