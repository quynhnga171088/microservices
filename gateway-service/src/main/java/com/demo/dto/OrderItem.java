package com.demo.dto;

/**
 * Một đơn hàng trong danh sách trả về.
 */
public record OrderItem(
        String orderId,
        String productName,
        int quantity,
        double totalPrice,
        String status
) {}
