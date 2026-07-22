package com.demo.event;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Event được publish lên Kafka khi user đăng nhập thành công.
 *
 * Dùng String cho timestamp thay vì LocalDateTime để tránh phụ thuộc
 * vào JavaTimeModule của Jackson — JsonSerializer serialize trực tiếp
 * không cần module bổ sung.
 *
 * auth-service publish → Kafka topic "user-events" → order-service consume
 */
public record UserLoggedInEvent(
        Long userId,
        String email,
        String role,
        String fullName,
        String timestamp          // ISO-8601 string: "2026-07-22T14:48:18"
) {
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    /** Factory method — tự set timestamp hiện tại dưới dạng ISO string */
    public static UserLoggedInEvent of(Long userId, String email, String role, String fullName) {
        return new UserLoggedInEvent(
                userId,
                email,
                role,
                fullName,
                LocalDateTime.now().format(FORMATTER)
        );
    }
}
