package com.ecommerce.shipping.model;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Entity
@Table(name = "shipments")
public class Shipment {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @NotBlank
    @Column(unique = true)
    private String shipmentId;
    
    @NotBlank
    private String orderId;
    
    @NotBlank
    private String customerId;
    
    @NotBlank
    private String carrierName;
    
    @NotBlank
    private String trackingNumber;
    
    @NotBlank
    private String shippingAddress;
    
    @NotBlank
    private String shippingCity;
    
    @NotBlank
    private String shippingZip;
    
    @NotBlank
    private String shippingCountry;
    
    @Enumerated(EnumType.STRING)
    private ShipmentStatus status;
    
    @Enumerated(EnumType.STRING)
    private ShippingMethod method;
    
    private Double shippingCost;
    
    private Double weight;
    
    private String dimensions;
    
    @Column(columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    private LocalDateTime createdAt;
    
    @Column(columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    private LocalDateTime updatedAt;
    
    private LocalDateTime shippingDate;
    
    private LocalDateTime estimatedDeliveryDate;
    
    private LocalDateTime actualDeliveryDate;
    
    // Enums
    public enum ShipmentStatus {
        CREATED, PICKED_UP, IN_TRANSIT, OUT_FOR_DELIVERY, DELIVERED, 
        DELIVERY_FAILED, RETURNED, CANCELLED
    }
    
    public enum ShippingMethod {
        STANDARD, EXPRESS, OVERNIGHT, SAME_DAY
    }
    
    // Constructors
    public Shipment() {}
    
    public Shipment(String shipmentId, String orderId, String customerId,
                   String carrierName, String shippingAddress, String shippingCity,
                   String shippingZip, String shippingCountry, ShippingMethod method,
                   Double shippingCost, Double weight) {
        this.shipmentId = shipmentId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.carrierName = carrierName;
        this.shippingAddress = shippingAddress;
        this.shippingCity = shippingCity;
        this.shippingZip = shippingZip;
        this.shippingCountry = shippingCountry;
        this.method = method;
        this.shippingCost = shippingCost;
        this.weight = weight;
        this.status = ShipmentStatus.CREATED;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }
    
    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
    
    // Helper methods
    public boolean isDelivered() {
        return status == ShipmentStatus.DELIVERED;
    }
    
    public boolean isInTransit() {
        return status == ShipmentStatus.IN_TRANSIT || status == ShipmentStatus.OUT_FOR_DELIVERY;
    }
    
    public void markAsPickedUp() {
        this.status = ShipmentStatus.PICKED_UP;
        this.shippingDate = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        // Calculate estimated delivery based on shipping method
        this.estimatedDeliveryDate = calculateEstimatedDelivery();
    }
    
    public void markAsInTransit() {
        this.status = ShipmentStatus.IN_TRANSIT;
        this.updatedAt = LocalDateTime.now();
    }
    
    public void markAsOutForDelivery() {
        this.status = ShipmentStatus.OUT_FOR_DELIVERY;
        this.updatedAt = LocalDateTime.now();
    }
    
    public void markAsDelivered() {
        this.status = ShipmentStatus.DELIVERED;
        this.actualDeliveryDate = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }
    
    public void markAsDeliveryFailed() {
        this.status = ShipmentStatus.DELIVERY_FAILED;
        this.updatedAt = LocalDateTime.now();
    }
    
    private LocalDateTime calculateEstimatedDelivery() {
        LocalDateTime base = shippingDate != null ? shippingDate : LocalDateTime.now();
        return switch (method) {
            case SAME_DAY -> base.plusHours(8);
            case OVERNIGHT -> base.plusDays(1);
            case EXPRESS -> base.plusDays(2);
            case STANDARD -> base.plusDays(5);
        };
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getShipmentId() { return shipmentId; }
    public void setShipmentId(String shipmentId) { this.shipmentId = shipmentId; }
    
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getCarrierName() { return carrierName; }
    public void setCarrierName(String carrierName) { this.carrierName = carrierName; }
    
    public String getTrackingNumber() { return trackingNumber; }
    public void setTrackingNumber(String trackingNumber) { this.trackingNumber = trackingNumber; }
    
    public String getShippingAddress() { return shippingAddress; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }
    
    public String getShippingCity() { return shippingCity; }
    public void setShippingCity(String shippingCity) { this.shippingCity = shippingCity; }
    
    public String getShippingZip() { return shippingZip; }
    public void setShippingZip(String shippingZip) { this.shippingZip = shippingZip; }
    
    public String getShippingCountry() { return shippingCountry; }
    public void setShippingCountry(String shippingCountry) { this.shippingCountry = shippingCountry; }
    
    public ShipmentStatus getStatus() { return status; }
    public void setStatus(ShipmentStatus status) { this.status = status; }
    
    public ShippingMethod getMethod() { return method; }
    public void setMethod(ShippingMethod method) { this.method = method; }
    
    public Double getShippingCost() { return shippingCost; }
    public void setShippingCost(Double shippingCost) { this.shippingCost = shippingCost; }
    
    public Double getWeight() { return weight; }
    public void setWeight(Double weight) { this.weight = weight; }
    
    public String getDimensions() { return dimensions; }
    public void setDimensions(String dimensions) { this.dimensions = dimensions; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    
    public LocalDateTime getShippingDate() { return shippingDate; }
    public void setShippingDate(LocalDateTime shippingDate) { this.shippingDate = shippingDate; }
    
    public LocalDateTime getEstimatedDeliveryDate() { return estimatedDeliveryDate; }
    public void setEstimatedDeliveryDate(LocalDateTime estimatedDeliveryDate) { this.estimatedDeliveryDate = estimatedDeliveryDate; }
    
    public LocalDateTime getActualDeliveryDate() { return actualDeliveryDate; }
    public void setActualDeliveryDate(LocalDateTime actualDeliveryDate) { this.actualDeliveryDate = actualDeliveryDate; }
}
