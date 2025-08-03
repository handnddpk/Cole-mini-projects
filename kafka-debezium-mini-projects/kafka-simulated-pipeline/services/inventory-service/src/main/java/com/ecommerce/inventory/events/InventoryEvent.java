package com.ecommerce.inventory.events;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InventoryEvent {
    
    @JsonProperty("event_id")
    private String eventId;
    
    @JsonProperty("product_id")
    private String productId;
    
    @JsonProperty("product_name")
    private String productName;
    
    @JsonProperty("category")
    private String category;
    
    @JsonProperty("event_type")
    private String eventType;
    
    @JsonProperty("quantity_before")
    private Integer quantityBefore;
    
    @JsonProperty("quantity_after")
    private Integer quantityAfter;
    
    @JsonProperty("reserved_quantity")
    private Integer reservedQuantity;
    
    @JsonProperty("available_quantity")
    private Integer availableQuantity;
    
    @JsonProperty("reorder_threshold")
    private Integer reorderThreshold;
    
    @JsonProperty("location")
    private String location;
    
    @JsonProperty("supplier")
    private String supplier;
    
    @JsonProperty("reason")
    private String reason;
    
    @JsonProperty("order_id")
    private String orderId;
    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;
    
    // Event types
    public static final String STOCK_UPDATED = "STOCK_UPDATED";
    public static final String STOCK_RESERVED = "STOCK_RESERVED";
    public static final String STOCK_RELEASED = "STOCK_RELEASED";
    public static final String LOW_STOCK_ALERT = "LOW_STOCK_ALERT";
    public static final String REORDER_REQUESTED = "REORDER_REQUESTED";
    public static final String PRODUCT_ADDED = "PRODUCT_ADDED";
    public static final String PRODUCT_REMOVED = "PRODUCT_REMOVED";
    
    // Constructors
    public InventoryEvent() {}
    
    public InventoryEvent(String eventId, String productId, String productName, 
                         String category, String eventType, Integer quantityBefore, 
                         Integer quantityAfter, Integer reservedQuantity, 
                         Integer availableQuantity, Integer reorderThreshold,
                         String location, String supplier, String reason, 
                         String orderId, LocalDateTime timestamp) {
        this.eventId = eventId;
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.eventType = eventType;
        this.quantityBefore = quantityBefore;
        this.quantityAfter = quantityAfter;
        this.reservedQuantity = reservedQuantity;
        this.availableQuantity = availableQuantity;
        this.reorderThreshold = reorderThreshold;
        this.location = location;
        this.supplier = supplier;
        this.reason = reason;
        this.orderId = orderId;
        this.timestamp = timestamp;
    }
    
    // Static factory methods
    public static InventoryEvent stockUpdated(String productId, String productName, String category,
                                            Integer quantityBefore, Integer quantityAfter,
                                            Integer reservedQuantity, Integer reorderThreshold,
                                            String location, String supplier, String reason) {
        return new InventoryEvent(
            java.util.UUID.randomUUID().toString(),
            productId, productName, category, STOCK_UPDATED,
            quantityBefore, quantityAfter, reservedQuantity,
            quantityAfter - reservedQuantity, reorderThreshold,
            location, supplier, reason, null, LocalDateTime.now()
        );
    }
    
    public static InventoryEvent stockReserved(String productId, String productName, String category,
                                             Integer reservedQuantity, Integer availableQuantity,
                                             Integer reorderThreshold, String location, 
                                             String supplier, String orderId) {
        return new InventoryEvent(
            java.util.UUID.randomUUID().toString(),
            productId, productName, category, STOCK_RESERVED,
            null, null, reservedQuantity, availableQuantity, reorderThreshold,
            location, supplier, "Reserved for order", orderId, LocalDateTime.now()
        );
    }
    
    public static InventoryEvent lowStockAlert(String productId, String productName, String category,
                                             Integer currentQuantity, Integer reorderThreshold,
                                             String location, String supplier) {
        return new InventoryEvent(
            java.util.UUID.randomUUID().toString(),
            productId, productName, category, LOW_STOCK_ALERT,
            null, currentQuantity, null, currentQuantity, reorderThreshold,
            location, supplier, "Stock below threshold", null, LocalDateTime.now()
        );
    }
    
    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    
    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }
    
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    
    public Integer getQuantityBefore() { return quantityBefore; }
    public void setQuantityBefore(Integer quantityBefore) { this.quantityBefore = quantityBefore; }
    
    public Integer getQuantityAfter() { return quantityAfter; }
    public void setQuantityAfter(Integer quantityAfter) { this.quantityAfter = quantityAfter; }
    
    public Integer getReservedQuantity() { return reservedQuantity; }
    public void setReservedQuantity(Integer reservedQuantity) { this.reservedQuantity = reservedQuantity; }
    
    public Integer getAvailableQuantity() { return availableQuantity; }
    public void setAvailableQuantity(Integer availableQuantity) { this.availableQuantity = availableQuantity; }
    
    public Integer getReorderThreshold() { return reorderThreshold; }
    public void setReorderThreshold(Integer reorderThreshold) { this.reorderThreshold = reorderThreshold; }
    
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    
    public String getSupplier() { return supplier; }
    public void setSupplier(String supplier) { this.supplier = supplier; }
    
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
}
