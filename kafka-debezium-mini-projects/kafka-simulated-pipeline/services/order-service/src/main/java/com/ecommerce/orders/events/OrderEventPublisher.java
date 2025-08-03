package com.ecommerce.orders.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {
    
    private static final Logger logger = LoggerFactory.getLogger(OrderEventPublisher.class);
    private static final String ORDER_TOPIC = "orders";
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public void publishOrderEvent(OrderEvent event) {
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            String key = event.getOrderId().toString();
            
            kafkaTemplate.send(ORDER_TOPIC, key, eventJson)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("Published order event: {} for order: {}", 
                            event.getEventType(), event.getOrderId());
                    } else {
                        logger.error("Failed to publish order event: {} for order: {}", 
                            event.getEventType(), event.getOrderId(), ex);
                    }
                });
                
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize order event: {}", event, e);
        }
    }
}
