package com.demo.consumer;

import com.demo.dto.OrderItem;
import com.demo.dto.OrderRequest;
import com.demo.dto.OrderResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Nhận yêu cầu danh sách đơn hàng từ gateway-service qua topic "order-requests".
 *
 * Luồng xử lý:
 * 1. Nhận ConsumerRecord<String, OrderRequest>
 * 2. Đọc correlationId và userId từ request
 * 3. Giả lập danh sách đơn hàng của user đó
 * 4. Đóng gói vào OrderResponse (kèm correlationId)
 * 5. Gửi reply về topic "gateway-order-replies"
 *
 * containerFactory: "orderRequestContainerFactory"
 * → Dùng factory riêng cho OrderRequest, tránh xung đột với
 *   kafkaListenerContainerFactory (dùng cho UserLoggedInEvent)
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderRequestConsumer {

    private final KafkaTemplate<String, OrderResponse> orderResponseKafkaTemplate;

    private static final String REPLY_TOPIC = "gateway-order-replies";

    @KafkaListener(
            topics = "order-requests",
            containerFactory = "orderRequestContainerFactory"
    )
    public void handleOrderRequest(ConsumerRecord<String, OrderRequest> record) {
        OrderRequest request = record.value();

        if (request == null) {
            log.warn("[OrderRequest] Received null request, offset={}", record.offset());
            return;
        }

        log.info("[OrderRequest] Processing: correlationId={}, userId={}, requestedBy={}",
                request.correlationId(), request.userId(), request.requestedBy());

        // Giả lập danh sách đơn hàng của userId
        List<OrderItem> orders = buildFakeOrders(request.userId());

        OrderResponse response = new OrderResponse(
                request.correlationId(),
                request.userId(),
                orders
        );

        // Gửi reply về gateway-service
        orderResponseKafkaTemplate.send(REPLY_TOPIC, request.correlationId(), response);

        log.info("[OrderRequest] Replied to gateway: correlationId={}, orderCount={}",
                request.correlationId(), orders.size());
    }

    /**
     * Giả lập dữ liệu đơn hàng.
     * Thực tế: thay bằng truy vấn database theo userId.
     */
    private List<OrderItem> buildFakeOrders(String userId) {
        return List.of(
                new OrderItem(
                        UUID.randomUUID().toString(),
                        "Laptop Dell XPS 15",
                        1,
                        35_990_000.0,
                        "DELIVERED"
                ),
                new OrderItem(
                        UUID.randomUUID().toString(),
                        "Chuột Logitech MX Master 3",
                        2,
                        2_890_000.0,
                        "SHIPPED"
                ),
                new OrderItem(
                        UUID.randomUUID().toString(),
                        "Bàn phím Keychron K2",
                        1,
                        2_190_000.0,
                        "PROCESSING"
                )
        );
    }
}
