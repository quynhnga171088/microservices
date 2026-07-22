package com.demo.service;

import com.demo.config.KafkaProducerConfig;
import com.demo.dto.LoginRequest;
import com.demo.dto.LoginResponse;
import com.demo.entity.User;
import com.demo.event.UserLoggedInEvent;
import com.demo.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuthService {

    private final UserRepository userRepository;
    private final BCryptPasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    // Spring Boot auto-configures KafkaTemplate<Object, Object> từ application.yml
    // Dùng wildcard ? để match với bean đó
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Xử lý login:
     * 1. Tìm user theo email trong DB
     * 2. Kiểm tra password bằng BCrypt
     * 3. Kiểm tra status == ACTIVE
     * 4. Tạo JWT token
     * 5. Publish UserLoggedInEvent lên Kafka (async, không block login nếu Kafka down)
     *
     * @throws ResponseStatusException 401 nếu sai credentials
     * @throws ResponseStatusException 403 nếu account bị khóa/inactive
     */
    public LoginResponse login(LoginRequest request) {
        log.info("Login attempt for email: {}", request.getEmail());

        // Bước 1: Tìm user theo email
        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> {
                    log.warn("Login failed — email not found: {}", request.getEmail());
                    // Trả về 401 chung để tránh user enumeration attack
                    return new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid email or password");
                });

        // Bước 2: Verify password (BCrypt compare)
        if (!passwordEncoder.matches(request.getPassword(), user.getPasswordHash())) {
            log.warn("Login failed — wrong password for email: {}", request.getEmail());
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid email or password");
        }

        // Bước 3: Kiểm tra status
        if (!"ACTIVE".equalsIgnoreCase(user.getStatus())) {
            log.warn("Login blocked — account status is '{}' for email: {}", user.getStatus(), request.getEmail());
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "Account is not active. Current status: " + user.getStatus());
        }

        // Bước 4: Tạo JWT token
        String token = jwtService.generateToken(user);
        log.info("Login successful for email: {} (role: {})", user.getEmail(), user.getRole());

        // Bước 5: Publish event lên Kafka (bất đồng bộ)
        // Dùng try-catch để login vẫn thành công dù Kafka tạm thời down
        publishLoginEvent(user);

        return new LoginResponse(token, user.getRole(), user.getFullName(), user.getEmail());
    }

    /**
     * Publish UserLoggedInEvent lên Kafka topic "user-events".
     *
     * Key = email (dùng để Kafka routing theo partition nếu có nhiều partition sau này)
     * Value = UserLoggedInEvent (JSON serialized)
     *
     * Không throw exception ra ngoài — login không bị block nếu Kafka down.
     */
    private void publishLoginEvent(User user) {
        try {
            UserLoggedInEvent event = UserLoggedInEvent.of(
                    user.getId(),
                    user.getEmail(),
                    user.getRole(),
                    user.getFullName()
            );

            kafkaTemplate.send(KafkaProducerConfig.USER_EVENTS_TOPIC, user.getEmail(), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish login event for user {}: {}", user.getEmail(), ex.getMessage());
                        } else {
                            log.info("Published login event for user {} → topic={}, partition={}, offset={}",
                                    user.getEmail(),
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            // Log nhưng không propagate — login vẫn thành công
            log.error("Unexpected error publishing login event for {}: {}", user.getEmail(), e.getMessage());
        }
    }
}
