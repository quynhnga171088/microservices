package com.demo.event;

import java.time.LocalDateTime;

/**
 * Event nhận từ Kafka topic "user-events" khi user login thành công.
 *
 * Phải có cùng structure với UserLoggedInEvent của auth-service.
 * Trong production nên đặt chung vào shared-library Maven module.
 *
 * Dùng Java Record (immutable, compatible với JSON deserialization).
 */
public record UserLoggedInEvent(
        Long userId,
        String email,
        String role,
        String fullName,
        String timestamp
) {
    // Record cần no-arg constructor cho Jackson deserialization
    // Jackson sẽ dùng canonical constructor của record tự động
}
