package com.demo.config;

import com.demo.dto.OrderRequest;
import com.demo.dto.OrderResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Cấu hình Kafka cho luồng Request/Reply trong order-service.
 *
 * Consumer: nhận OrderRequest từ topic "order-requests"
 *           → Dùng factory riêng (orderRequestConsumerFactory) để tránh
 *             xung đột với KafkaConsumerConfig (đang dùng cho UserLoggedInEvent).
 *
 * Producer: gửi OrderResponse về topic "gateway-order-replies"
 *           → KafkaTemplate<String, OrderResponse>
 *
 * Bean names khác với KafkaConsumerConfig để Spring không nhầm lẫn:
 *   "orderRequestConsumerFactory"    (thay vì "consumerFactory")
 *   "orderRequestContainerFactory"   (thay vì "kafkaListenerContainerFactory")
 */
@Configuration
public class KafkaOrderRequestConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Consumer group riêng cho luồng request-reply
    private static final String ORDER_REQUEST_GROUP = "order-request-handler-group";

    // ─────────────────────── CONSUMER (nhận OrderRequest) ───────────────────────

    @Bean
    public ConsumerFactory<String, OrderRequest> orderRequestConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, ORDER_REQUEST_GROUP);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Deserialize thẳng về OrderRequest, không dùng __TypeId__ header
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.demo.dto.OrderRequest");
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderRequest> orderRequestContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderRequest> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderRequestConsumerFactory());
        return factory;
    }

    // ─────────────────────── PRODUCER (gửi OrderResponse) ──────────────────────

    @Bean
    public ProducerFactory<String, OrderResponse> orderResponseProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderResponse> orderResponseKafkaTemplate() {
        return new KafkaTemplate<>(orderResponseProducerFactory());
    }
}
