package com.ecommerce.inventory.service;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ecommerce.inventory.events.InventoryEventPublisher;
import com.ecommerce.inventory.model.InventoryItem;
import com.ecommerce.inventory.repository.InventoryRepository;

@Service
@Transactional
public class InventoryService {
    
    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);
    
    @Autowired
    private InventoryRepository inventoryRepository;
    
    @Autowired
    private InventoryEventPublisher eventPublisher;
    
    public List<InventoryItem> getAllItems() {
        return inventoryRepository.findAll();
    }
    
    public Optional<InventoryItem> getItemByProductId(String productId) {
        return inventoryRepository.findByProductId(productId);
    }
    
    public List<InventoryItem> getItemsByCategory(String category) {
        return inventoryRepository.findByCategory(category);
    }
    
    public List<InventoryItem> getLowStockItems() {
        return inventoryRepository.findLowStockItems();
    }
    
    public InventoryItem addProduct(InventoryItem item) {
        InventoryItem savedItem = inventoryRepository.save(item);
        eventPublisher.publishProductAdded(savedItem);
        logger.info("Added new product to inventory: {}", savedItem.getProductId());
        return savedItem;
    }
    
    public InventoryItem updateStock(String productId, Integer newQuantity, String reason) {
        Optional<InventoryItem> itemOpt = inventoryRepository.findByProductId(productId);
        if (itemOpt.isPresent()) {
            InventoryItem item = itemOpt.get();
            Integer previousQuantity = item.getQuantity();
            item.setQuantity(newQuantity);
            
            InventoryItem updatedItem = inventoryRepository.save(item);
            eventPublisher.publishStockUpdated(updatedItem, previousQuantity, reason);
            
            // Check if low stock alert needed
            if (updatedItem.isLowStock()) {
                eventPublisher.publishLowStockAlert(updatedItem);
            }
            
            logger.info("Updated stock for product {}: {} -> {}", 
                       productId, previousQuantity, newQuantity);
            return updatedItem;
        }
        throw new RuntimeException("Product not found: " + productId);
    }
    
    public boolean reserveStock(String productId, Integer quantity, String orderId) {
        Optional<InventoryItem> itemOpt = inventoryRepository.findAvailableItem(productId, quantity);
        if (itemOpt.isPresent()) {
            InventoryItem item = itemOpt.get();
            item.reserveStock(quantity);
            
            InventoryItem updatedItem = inventoryRepository.save(item);
            eventPublisher.publishStockReserved(updatedItem, orderId);
            
            // Check if low stock alert needed after reservation
            if (updatedItem.isLowStock()) {
                eventPublisher.publishLowStockAlert(updatedItem);
            }
            
            logger.info("Reserved {} units of product {} for order {}", 
                       quantity, productId, orderId);
            return true;
        }
        
        logger.warn("Could not reserve {} units of product {} - insufficient stock", 
                   quantity, productId);
        return false;
    }
    
    public void releaseStock(String productId, Integer quantity, String orderId) {
        Optional<InventoryItem> itemOpt = inventoryRepository.findByProductId(productId);
        if (itemOpt.isPresent()) {
            InventoryItem item = itemOpt.get();
            item.releaseStock(quantity);
            
            InventoryItem updatedItem = inventoryRepository.save(item);
            logger.info("Released {} units of product {} from order {}", 
                       quantity, productId, orderId);
        }
    }
    
    public void confirmStockUsage(String productId, Integer quantity, String orderId) {
        Optional<InventoryItem> itemOpt = inventoryRepository.findByProductId(productId);
        if (itemOpt.isPresent()) {
            InventoryItem item = itemOpt.get();
            Integer previousQuantity = item.getQuantity();
            item.removeStock(quantity);
            
            InventoryItem updatedItem = inventoryRepository.save(item);
            eventPublisher.publishStockUpdated(updatedItem, previousQuantity, 
                                             "Stock confirmed for order: " + orderId);
            logger.info("Confirmed usage of {} units of product {} for order {}", 
                       quantity, productId, orderId);
        }
    }
    
    public void addStock(String productId, Integer quantity, String reason) {
        Optional<InventoryItem> itemOpt = inventoryRepository.findByProductId(productId);
        if (itemOpt.isPresent()) {
            InventoryItem item = itemOpt.get();
            Integer previousQuantity = item.getQuantity();
            item.addStock(quantity);
            
            InventoryItem updatedItem = inventoryRepository.save(item);
            eventPublisher.publishStockUpdated(updatedItem, previousQuantity, reason);
            logger.info("Added {} units to product {}: {}", quantity, productId, reason);
        }
    }
    
    public List<InventoryItem> searchItems(String searchTerm) {
        return inventoryRepository.searchByNameOrCategory(searchTerm);
    }
    
    // Scheduled task to check for low stock and trigger reorders
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void checkLowStockAndReorder() {
        List<InventoryItem> lowStockItems = getLowStockItems();
        
        for (InventoryItem item : lowStockItems) {
            logger.info("Low stock detected for product {}: available={}, threshold={}", 
                       item.getProductId(), item.getAvailableQuantity(), item.getReorderThreshold());
            
            // Trigger reorder request
            eventPublisher.publishReorderRequested(item);
        }
        
        if (!lowStockItems.isEmpty()) {
            logger.info("Processed {} low stock items for reorder", lowStockItems.size());
        }
    }
    
    // Simulated restock process
    public void processRestock(String productId, Integer quantity) {
        addStock(productId, quantity, "Automatic restock");
    }
}
