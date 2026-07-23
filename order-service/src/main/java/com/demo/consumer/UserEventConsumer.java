package com.demo.consumer;

import com.demo.event.UserLoggedInEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer — lắng nghe events từ topic "user-events".
 *
 * Trong demo này: log audit trail khi user login.
 * Trong production: có thể lưu vào DB audit_logs, gửi notification, v.v.
 *
 * group-id: order-service-group-v2
 * → Kafka đảm bảo mỗi message chỉ được xử lý bởi 1 instance trong group
 * → Scale horizontal: chạy 2 order-service → load balanced
 *
 * Dùng ConsumerRecord<K,V> thay vì @Payload + @Header riêng lẻ
 * → Tránh lỗi KafkaHeaders constant thay đổi giữa các version
 */
@Component
public class UserEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(UserEventConsumer.class);

    /**
     * Xử lý UserLoggedInEvent từ Kafka.
     *
     * ConsumerRecord chứa toàn bộ thông tin: key, value, partition, offset, timestamp...
     * → Đơn giản, ổn định, không phụ thuộc vào KafkaHeaders constant
     */
    @KafkaListener(
            topics = "user-events",
            containerFactory = "userEventContainerFactory"
    )
    public void handleUserLoggedIn(ConsumerRecord<String, UserLoggedInEvent> record) {
        UserLoggedInEvent event = record.value();

        if (event == null) {
            log.warn("[AUDIT] Received null event from topic=user-events, offset={}", record.offset());
            return;
        }

        log.info("""
                ╔══════════════════════════════════════════╗
                ║  [AUDIT] User Login Event Received       ║
                ╠══════════════════════════════════════════╣
                ║  userId   : {}
                ║  email    : {}
                ║  role     : {}
                ║  fullName : {}
                ║  timestamp: {}
                ║  partition: {}  offset: {}
                ╚══════════════════════════════════════════╝
                """,
                event.userId(),
                event.email(),
                event.role(),
                event.fullName(),
                event.timestamp(),
                record.partition(),
                record.offset()
        );

        // TODO (Phase 3b): Lưu vào audit_logs table
        // TODO (Phase 3c): Trigger Saga nếu cần orchestrate nhiều services
    }
}
