package com.demo.service;

import com.demo.dto.OrderResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Trung tâm điều phối correlation ID cho luồng Request/Reply qua Kafka.
 *
 * Cơ chế hoạt động:
 * 1. Controller gọi waitForReply(correlationId)
 *    → Tạo CompletableFuture, lưu vào pendingReplies map
 *    → Trả về Mono.fromFuture() để WebFlux chờ (non-blocking)
 *
 * 2. OrderReplyConsumer nhận message từ topic "gateway-order-replies"
 *    → Gọi completeReply(correlationId, response)
 *    → Tìm Future trong map, gọi future.complete()
 *    → Mono trong controller tự unblock và trả kết quả về client
 *
 * 3. Nếu sau TIMEOUT_SECONDS giây vẫn chưa nhận được reply
 *    → Future bị cancel (cleanup), controller trả về 504 Gateway Timeout
 *
 * Thread-safety: ConcurrentHashMap đảm bảo an toàn khi nhiều request đồng thời.
 */
@Service
@Slf4j
public class OrderReplyService {

    private static final int TIMEOUT_SECONDS = 10;

    /** Map: correlationId → CompletableFuture chờ reply */
    private final ConcurrentHashMap<String, CompletableFuture<OrderResponse>> pendingReplies =
            new ConcurrentHashMap<>();

    /**
     * Tạo một "chỗ chờ" cho reply và trả về Mono để WebFlux subscribe.
     * Mono sẽ emit value khi completeReply() được gọi với đúng correlationId.
     */
    public Mono<OrderResponse> waitForReply(String correlationId) {
        CompletableFuture<OrderResponse> future = new CompletableFuture<OrderResponse>()
                .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        pendingReplies.put(correlationId, future);
        log.debug("[Reply] Waiting for correlationId={}, pending={}", correlationId, pendingReplies.size());

        return Mono.fromFuture(future)
                .doFinally(signal -> {
                    // Dọn dẹp map dù success, error hay cancel
                    pendingReplies.remove(correlationId);
                    log.debug("[Reply] Cleaned up correlationId={}", correlationId);
                });
    }

    /**
     * Được gọi bởi OrderReplyConsumer khi nhận được message từ Kafka.
     * Complete future → Mono trong controller unblock và trả kết quả.
     */
    public void completeReply(String correlationId, OrderResponse response) {
        CompletableFuture<OrderResponse> future = pendingReplies.get(correlationId);

        if (future == null) {
            // Có thể đã timeout trước khi reply đến
            log.warn("[Reply] No pending future for correlationId={} (already timed out?)", correlationId);
            return;
        }

        log.debug("[Reply] Completing correlationId={}", correlationId);
        future.complete(response);
    }
}
