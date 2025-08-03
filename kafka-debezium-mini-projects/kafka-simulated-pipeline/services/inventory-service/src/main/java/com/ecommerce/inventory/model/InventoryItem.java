package com.ecommerce.inventory.model;

import javax.persistence.*;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Entity
@Table(name = "inventory_items")
public class InventoryItem {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @NotBlank
    @Column(unique = true)
    private String productId;
    
    @NotBlank
    private String productName;
    
    @NotBlank
    private String category;
    
    @NotNull
    @Min(0)
    private Integer quantity;
    
    @NotNull
    @Min(0)
    private Integer reservedQuantity = 0;
    
    @NotNull
    @Min(1)
    private Integer reorderThreshold;
    
    @NotNull
    @Min(1)
    private Integer reorderQuantity;
    
    @NotBlank
    private String location;
    
    @NotBlank
    private String supplier;
    
    @Column(columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    private LocalDateTime createdAt;
    
    @Column(columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    private LocalDateTime updatedAt;
    
    // Constructors
    public InventoryItem() {}
    
    public InventoryItem(String productId, String productName, String category, 
                        Integer quantity, Integer reorderThreshold, Integer reorderQuantity,
                        String location, String supplier) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.quantity = quantity;
        this.reorderThreshold = reorderThreshold;
        this.reorderQuantity = reorderQuantity;
        this.location = location;
        this.supplier = supplier;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }
    
    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
    
    // Helper methods
    public Integer getAvailableQuantity() {
        return quantity - reservedQuantity;
    }
    
    public boolean isLowStock() {
        return getAvailableQuantity() <= reorderThreshold;
    }
    
    public boolean canReserve(Integer amount) {
        return getAvailableQuantity() >= amount;
    }
    
    public void reserveStock(Integer amount) {
        if (canReserve(amount)) {
            this.reservedQuantity += amount;
        } else {
            throw new RuntimeException("Insufficient stock available");
        }
    }
    
    public void releaseStock(Integer amount) {
        this.reservedQuantity = Math.max(0, this.reservedQuantity - amount);
    }
    
    public void removeStock(Integer amount) {
        this.quantity = Math.max(0, this.quantity - amount);
        this.reservedQuantity = Math.max(0, this.reservedQuantity - amount);
    }
    
    public void addStock(Integer amount) {
        this.quantity += amount;
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    
    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }
    
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    
    public Integer getReservedQuantity() { return reservedQuantity; }
    public void setReservedQuantity(Integer reservedQuantity) { this.reservedQuantity = reservedQuantity; }
    
    public Integer getReorderThreshold() { return reorderThreshold; }
    public void setReorderThreshold(Integer reorderThreshold) { this.reorderThreshold = reorderThreshold; }
    
    public Integer getReorderQuantity() { return reorderQuantity; }
    public void setReorderQuantity(Integer reorderQuantity) { this.reorderQuantity = reorderQuantity; }
    
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    
    public String getSupplier() { return supplier; }
    public void setSupplier(String supplier) { this.supplier = supplier; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
