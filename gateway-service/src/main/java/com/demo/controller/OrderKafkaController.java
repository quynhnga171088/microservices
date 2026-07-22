package com.demo.controller;

import com.demo.dto.OrderRequest;
import com.demo.dto.OrderResponse;
import com.demo.service.OrderReplyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * REST endpoint cho luồng đồng bộ qua Kafka (Request/Reply pattern).
 *
 * GET /orders
 * ─ Yêu cầu JWT hợp lệ (JwtAuthFilter đã inject X-User-Id, X-User-Email)
 * ─ Gửi message đến topic "order-requests"
 * ─ Chờ reply từ topic "gateway-order-replies" (tối đa 10s)
 * ─ Trả danh sách đơn hàng về client
 *
 * Không cần khai báo route trong application.yml vì đây là local controller
 * (Spring WebFlux DispatcherHandler xử lý trực tiếp, không qua gateway routing).
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderKafkaController {

    private final KafkaTemplate<String, OrderRequest> orderRequestKafkaTemplate;
    private final OrderReplyService orderReplyService;

    private static final String ORDER_REQUESTS_TOPIC = "order-requests";

    /**
     * GET /orders
     *
     * Headers được inject bởi JwtAuthFilter:
     *   X-User-Id    — ID của user đã đăng nhập
     *   X-User-Email — Email của user
     */
    @GetMapping
    public Mono<ResponseEntity<OrderResponse>> getOrders(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader("X-User-Email") String userEmail) {

        String correlationId = UUID.randomUUID().toString();

        log.info("[Orders] Request from userId={}, email={}, correlationId={}",
                userId, userEmail, correlationId);

        OrderRequest request = new OrderRequest(correlationId, userId, userEmail);

        // Gửi request sang order-service qua Kafka (non-blocking)
        orderRequestKafkaTemplate.send(ORDER_REQUESTS_TOPIC, correlationId, request);

        log.info("[Orders] Sent to Kafka topic={}, correlationId={}", ORDER_REQUESTS_TOPIC, correlationId);

        // Chờ reply — Mono sẽ unblock khi OrderReplyConsumer nhận được message
        return orderReplyService.waitForReply(correlationId)
                .map(ResponseEntity::ok)
                .onErrorResume(TimeoutException.class, ex -> {
                    log.error("[Orders] Timeout waiting for reply, correlationId={}", correlationId);
                    return Mono.just(ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build());
                })
                .onErrorResume(ex -> {
                    log.error("[Orders] Unexpected error for correlationId={}: {}", correlationId, ex.getMessage());
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }
}
