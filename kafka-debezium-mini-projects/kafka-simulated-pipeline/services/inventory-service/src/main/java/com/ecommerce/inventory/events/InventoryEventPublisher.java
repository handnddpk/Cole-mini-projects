package com.ecommerce.inventory.events;

import com.ecommerce.inventory.model.InventoryItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.LocalDateTime;

@Component
public class InventoryEventPublisher {
    
    private static final Logger logger = LoggerFactory.getLogger(InventoryEventPublisher.class);
    
    private static final String INVENTORY_TOPIC = "inventory-events";
    
    @Autowired
    private KafkaTemplate<String, InventoryEvent> kafkaTemplate;
    
    public void publishStockUpdated(InventoryItem item, Integer previousQuantity, String reason) {
        InventoryEvent event = InventoryEvent.stockUpdated(
            item.getProductId(),
            item.getProductName(),
            item.getCategory(),
            previousQuantity,
            item.getQuantity(),
            item.getReservedQuantity(),
            item.getReorderThreshold(),
            item.getLocation(),
            item.getSupplier(),
            reason
        );
        
        publishEvent(event);
    }
    
    public void publishStockReserved(InventoryItem item, String orderId) {
        InventoryEvent event = InventoryEvent.stockReserved(
            item.getProductId(),
            item.getProductName(),
            item.getCategory(),
            item.getReservedQuantity(),
            item.getAvailableQuantity(),
            item.getReorderThreshold(),
            item.getLocation(),
            item.getSupplier(),
            orderId
        );
        
        publishEvent(event);
    }
    
    public void publishLowStockAlert(InventoryItem item) {
        InventoryEvent event = InventoryEvent.lowStockAlert(
            item.getProductId(),
            item.getProductName(),
            item.getCategory(),
            item.getAvailableQuantity(),
            item.getReorderThreshold(),
            item.getLocation(),
            item.getSupplier()
        );
        
        publishEvent(event);
    }
    
    public void publishReorderRequested(InventoryItem item) {
        InventoryEvent event = new InventoryEvent(
            java.util.UUID.randomUUID().toString(),
            item.getProductId(),
            item.getProductName(),
            item.getCategory(),
            InventoryEvent.REORDER_REQUESTED,
            null,
            null,
            item.getReservedQuantity(),
            item.getAvailableQuantity(),
            item.getReorderThreshold(),
            item.getLocation(),
            item.getSupplier(),
            "Automatic reorder request",
            null,
            LocalDateTime.now()
        );
        
        publishEvent(event);
    }
    
    public void publishProductAdded(InventoryItem item) {
        InventoryEvent event = new InventoryEvent(
            java.util.UUID.randomUUID().toString(),
            item.getProductId(),
            item.getProductName(),
            item.getCategory(),
            InventoryEvent.PRODUCT_ADDED,
            0,
            item.getQuantity(),
            item.getReservedQuantity(),
            item.getAvailableQuantity(),
            item.getReorderThreshold(),
            item.getLocation(),
            item.getSupplier(),
            "New product added to inventory",
            null,
            LocalDateTime.now()
        );
        
        publishEvent(event);
    }
    
    private void publishEvent(InventoryEvent event) {
        try {
            ListenableFuture<SendResult<String, InventoryEvent>> future = 
                kafkaTemplate.send(INVENTORY_TOPIC, event.getProductId(), event);
            
            future.addCallback(new ListenableFutureCallback<SendResult<String, InventoryEvent>>() {
                @Override
                public void onSuccess(SendResult<String, InventoryEvent> result) {
                    logger.info("Successfully published inventory event: {} for product: {}", 
                               event.getEventType(), event.getProductId());
                }
                
                @Override
                public void onFailure(Throwable ex) {
                    logger.error("Failed to publish inventory event: {} for product: {}", 
                                event.getEventType(), event.getProductId(), ex);
                }
            });
            
        } catch (Exception e) {
            logger.error("Error publishing inventory event: {} for product: {}", 
                        event.getEventType(), event.getProductId(), e);
        }
    }
}
