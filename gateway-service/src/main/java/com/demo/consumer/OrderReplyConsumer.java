package com.demo.consumer;

import com.demo.dto.OrderResponse;
import com.demo.service.OrderReplyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Nhận reply từ order-service qua topic "gateway-order-replies".
 *
 * Khi nhận được message:
 * → Đọc correlationId từ OrderResponse
 * → Gọi OrderReplyService.completeReply() để unblock CompletableFuture
 *   đang chờ trong OrderKafkaController
 *
 * containerFactory: chỉ rõ dùng factory của gateway (deserialize OrderResponse),
 *                   không dùng factory mặc định của order-service (nếu có).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderReplyConsumer {

    private final OrderReplyService orderReplyService;

    @KafkaListener(
            topics = "gateway-order-replies",
            containerFactory = "orderReplyContainerFactory"
    )
    public void handleOrderReply(ConsumerRecord<String, OrderResponse> record) {
        OrderResponse response = record.value();

        if (response == null) {
            log.warn("[GatewayReply] Received null reply from topic=gateway-order-replies, offset={}", record.offset());
            return;
        }

        log.info("[GatewayReply] Received reply: correlationId={}, userId={}, orderCount={}",
                response.correlationId(), response.userId(), response.orders().size());

        orderReplyService.completeReply(response.correlationId(), response);
    }
}
