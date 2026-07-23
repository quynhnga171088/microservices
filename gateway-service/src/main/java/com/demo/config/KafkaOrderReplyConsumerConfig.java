package com.demo.config;

import com.demo.dto.OrderResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer config cho gateway-service.
 *
 * Chịu trách nhiệm: nhận OrderResponse từ topic "gateway-order-replies"
 * → reply từ order-service sau khi xử lý xong "order-requests".
 *
 * @EnableKafka: bắt buộc để Spring kích hoạt @KafkaListener
 *               trong OrderReplyConsumer.
 *
 * Tên factory bean: "orderReplyContainerFactory"
 * → OrderReplyConsumer khai báo containerFactory = "orderReplyContainerFactory"
 */
@Configuration
@EnableKafka
public class KafkaOrderReplyConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String replyGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Bean
    public ConsumerFactory<String, OrderResponse> orderReplyConsumerFactory() {
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

    /**
     * Factory cho topic "gateway-order-replies" — deserialize OrderResponse.
     * Tên bean: "orderReplyContainerFactory"
     * → OrderReplyConsumer khai báo containerFactory = "orderReplyContainerFactory"
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderResponse> orderReplyContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderReplyConsumerFactory());
        return factory;
    }
}
