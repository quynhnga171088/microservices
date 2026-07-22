package com.demo.filter;

import com.demo.service.JwtService;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * JWT Authentication + Role Authorization Filter.
 *
 * Implements WebFilter (không phải GlobalFilter) để chạy trên MỌI request:
 * - GlobalFilter chỉ chạy khi có route khớp trong application.yml
 *   → local @RestController (như OrderKafkaController) sẽ KHÔNG được bảo vệ
 * - WebFilter chạy trước DispatcherHandler, bao phủ cả gateway routes
 *   lẫn local controllers trong cùng JVM
 *
 * Thứ tự xử lý:
 * 1. Path thuộc PUBLIC_PATHS (/auth/**) → skip, forward ngay
 * 2. Thiếu/sai format header Authorization → 401
 * 3. JWT invalid/expired → 401
 * 4. Kiểm tra Role — path yêu cầu role cụ thể nhưng user không có → 403
 * 5. Tất cả pass → inject X-User-* headers → forward tới service
 *
 * Downstream services nhận được headers:
 * X-User-Id — ID từ database
 * X-User-Role — STUDENT | TEACHER | ADMIN
 * X-User-Email — email (JWT subject)
 * X-User-Name — full name
 */
@Component
@Order(-1)
@RequiredArgsConstructor
@Slf4j
public class JwtAuthFilter implements WebFilter {

    private final JwtService jwtService;

    /**
     * Các path không cần JWT — prefix match.
     *
     * /actuator/ phải public: Consul gọi /actuator/health để health check,
     * không có JWT → nếu không exempt thì Consul đánh dấu service "critical"
     * → lb://gateway-service không resolve được → toàn bộ routing hỏng.
     */
    private static final List<String> PUBLIC_PATHS = List.of("/auth/", "/actuator/");

    /**
     * Route-level Role Authorization.
     *
     * Key : path prefix cần bảo vệ (phải kết thúc bằng "/")
     * Value: danh sách roles được phép — các role KHÔNG có trong list → 403
     *
     * Thêm rule mới vào đây khi cần protect thêm route.
     * Ví dụ:
     * "/reports/", List.of("ADMIN", "TEACHER", "STUDENT")
     * "/finance/", List.of("ADMIN")
     */
    private static final Map<String, List<String>> ROLE_REQUIRED_PATHS = Map.of(
            "/parts/", List.of("ADMIN"), // only ADMIN can call /parts/**
            "/users/", List.of("ADMIN", "TEACHER", "STUDENT") // only ADMIN and TEACHER can call /users/**
    );

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();

        // Bước 1: Skip authentication cho public paths
        if (isPublicPath(path)) {
            log.debug("Public path, skipping JWT check: {}", path);
            return chain.filter(exchange);
        }

        // Bước 2: Đọc Authorization header
        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            log.warn("Missing or invalid Authorization header for path: {}", path);
            return writeUnauthorized(exchange, "Missing Authorization header. Use: Authorization: Bearer <token>");
        }

        // Bước 3: Extract và validate JWT token
        String token = authHeader.substring(7); // cắt bỏ "Bearer "

        Claims claims;
        try {
            claims = jwtService.extractAllClaims(token);
        } catch (JwtException e) {
            log.warn("JWT validation failed for path {}: {}", path, e.getMessage());
            return writeUnauthorized(exchange, "Invalid or expired JWT token");
        }

        // Bước 4: Kiểm tra Role Authorization
        String userRole = String.valueOf(claims.get("role")); // "STUDENT" | "TEACHER" | "ADMIN"

        for (Map.Entry<String, List<String>> entry : ROLE_REQUIRED_PATHS.entrySet()) {
            if (path.startsWith(entry.getKey())) {
                List<String> allowedRoles = entry.getValue();
                if (!allowedRoles.contains(userRole)) {
                    log.warn("Access denied for '{}': user '{}' has role '{}', required one of: {}",
                            path, claims.getSubject(), userRole, allowedRoles);
                    return writeForbidden(exchange,
                            "Access denied. Required role: " + allowedRoles
                                    + ". Your role: " + userRole);
                }
                break; // path match rồi, không cần check tiếp
            }
        }

        // Bước 5: Tất cả pass — inject X-User-* headers cho downstream services
        // Xóa headers cũ trước để chống client inject headers giả mạo
        ServerHttpRequest mutatedRequest = exchange.getRequest().mutate()
                .headers(headers -> {
                    headers.remove("X-User-Id");
                    headers.remove("X-User-Role");
                    headers.remove("X-User-Email");
                    headers.remove("X-User-Name");
                })
                .header("X-User-Id", String.valueOf(claims.get("userId")))
                .header("X-User-Role", userRole)
                .header("X-User-Email", claims.getSubject())
                .header("X-User-Name", String.valueOf(claims.get("fullName")))
                .build();

        log.debug("JWT valid — forwarding '{}' for user: {} (role: {})", path, claims.getSubject(), userRole);

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }

    private boolean isPublicPath(String path) {
        return PUBLIC_PATHS.stream().anyMatch(path::startsWith);
    }

    /** 401 — Chưa xác thực (thiếu/sai token) */
    private Mono<Void> writeUnauthorized(ServerWebExchange exchange, String message) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        String body = """
                {
                    "status": 401,
                    "error": "Unauthorized",
                    "message": "%s"
                }
                """.formatted(message);

        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body.getBytes());
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    /** 403 — Đã xác thực nhưng không đủ quyền */
    private Mono<Void> writeForbidden(ServerWebExchange exchange, String message) {
        exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
        exchange.getResponse().getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        String body = """
                {
                    "status": 403,
                    "error": "Forbidden",
                    "message": "%s"
                }
                """.formatted(message);

        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body.getBytes());
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
