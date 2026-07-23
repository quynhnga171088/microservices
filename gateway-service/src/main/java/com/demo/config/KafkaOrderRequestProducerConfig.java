package com.demo.config;

import com.demo.dto.OrderRequest;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Producer config cho gateway-service.
 *
 * Chịu trách nhiệm: gửi OrderRequest sang topic "order-requests"
 * → order-service sẽ lắng nghe topic này và xử lý.
 *
 * Tách riêng khỏi consumer config để mỗi class chỉ có 1 trách nhiệm:
 * - File này   : biết cách GỬI message (Serializer, ProducerFactory, KafkaTemplate)
 * - KafkaOrderReplyConsumerConfig.java : biết cách NHẬN reply (Deserializer, ConsumerFactory, ContainerFactory)
 */
@Configuration
public class KafkaOrderRequestProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, OrderRequest> orderRequestProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Không gửi __TypeId__ header để tránh phụ thuộc class name giữa 2 service
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * KafkaTemplate dùng trong OrderKafkaController để gửi OrderRequest.
     * Bean name: "orderRequestKafkaTemplate" (inject theo tên hoặc kiểu)
     */
    @Bean
    public KafkaTemplate<String, OrderRequest> orderRequestKafkaTemplate() {
        return new KafkaTemplate<>(orderRequestProducerFactory());
    }
}
