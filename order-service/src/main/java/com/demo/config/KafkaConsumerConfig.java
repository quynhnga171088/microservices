package com.demo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonLoggingErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Cấu hình Kafka Consumer tường minh cho order-service.
 *
 * Dùng ErrorHandlingDeserializer bọc bên ngoài JsonDeserializer:
 * → Khi deserialize lỗi (type mismatch, JSON sai...) sẽ in log ra
 *   thay vì âm thầm drop message khiến listener không nhận được gì.
 *
 * @EnableKafka: bắt buộc phải có để Spring quét và khởi tạo container
 * cho các method có @KafkaListener (ví dụ UserEventConsumer). Thiếu
 * annotation này thì @KafkaListener sẽ không bao giờ được xử lý —
 * bean vẫn tồn tại nhưng không có container nào chạy, không log lỗi,
 * không có exception — rất khó nhận ra nếu chỉ đọc code.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ErrorHandlingDeserializer bọc bên ngoài:
        // Nếu value deserialize thất bại → ghi log ERROR thay vì crash/drop ngầm
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        // Deserializer thực sự bên trong ErrorHandlingDeserializer
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Không dùng __TypeId__ header → tự map thẳng về class này
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.demo.event.UserLoggedInEvent");
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Log rõ ràng khi có lỗi xử lý message
        factory.setCommonErrorHandler(new CommonLoggingErrorHandler());
        return factory;
    }
}
