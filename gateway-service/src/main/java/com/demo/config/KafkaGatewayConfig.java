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
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka config cho gateway-service — phục vụ luồng Request/Reply đồng bộ.
 *
 * Producer: gửi OrderRequest sang topic "order-requests"
 * Consumer: nhận OrderResponse từ topic "gateway-order-replies"
 *
 * @EnableKafka: bắt buộc để Spring kích hoạt @KafkaListener
 *               (trong OrderReplyConsumer).
 */
@Configuration
@EnableKafka
public class KafkaGatewayConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String replyGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    // ─────────────────────── PRODUCER (gửi OrderRequest) ───────────────────────

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

    @Bean
    public KafkaTemplate<String, OrderRequest> orderRequestKafkaTemplate() {
        return new KafkaTemplate<>(orderRequestProducerFactory());
    }

    // ─────────────────────── CONSUMER (nhận OrderResponse) ─────────────────────

    @Bean
    public ConsumerFactory<String, OrderResponse> orderResponseConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, replyGroupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Deserialize thẳng về OrderResponse, không dùng __TypeId__ header
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.demo.dto.OrderResponse");
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderResponse> orderResponseContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderResponseConsumerFactory());
        return factory;
    }
}
