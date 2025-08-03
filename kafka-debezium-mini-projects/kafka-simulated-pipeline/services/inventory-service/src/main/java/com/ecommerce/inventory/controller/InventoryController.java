package com.ecommerce.inventory.controller;

import com.ecommerce.inventory.model.InventoryItem;
import com.ecommerce.inventory.service.InventoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/inventory")
@CrossOrigin(origins = "*")
public class InventoryController {
    
    @Autowired
    private InventoryService inventoryService;
    
    @GetMapping
    public ResponseEntity<List<InventoryItem>> getAllItems() {
        List<InventoryItem> items = inventoryService.getAllItems();
        return ResponseEntity.ok(items);
    }
    
    @GetMapping("/{productId}")
    public ResponseEntity<InventoryItem> getItem(@PathVariable String productId) {
        Optional<InventoryItem> item = inventoryService.getItemByProductId(productId);
        return item.map(ResponseEntity::ok)
                  .orElse(ResponseEntity.notFound().build());
    }
    
    @GetMapping("/category/{category}")
    public ResponseEntity<List<InventoryItem>> getItemsByCategory(@PathVariable String category) {
        List<InventoryItem> items = inventoryService.getItemsByCategory(category);
        return ResponseEntity.ok(items);
    }
    
    @GetMapping("/low-stock")
    public ResponseEntity<List<InventoryItem>> getLowStockItems() {
        List<InventoryItem> items = inventoryService.getLowStockItems();
        return ResponseEntity.ok(items);
    }
    
    @GetMapping("/search")
    public ResponseEntity<List<InventoryItem>> searchItems(@RequestParam String q) {
        List<InventoryItem> items = inventoryService.searchItems(q);
        return ResponseEntity.ok(items);
    }
    
    @PostMapping
    public ResponseEntity<InventoryItem> addProduct(@Valid @RequestBody InventoryItem item) {
        InventoryItem savedItem = inventoryService.addProduct(item);
        return ResponseEntity.ok(savedItem);
    }
    
    @PutMapping("/{productId}/stock")
    public ResponseEntity<InventoryItem> updateStock(
            @PathVariable String productId,
            @RequestParam Integer quantity,
            @RequestParam(defaultValue = "Manual update") String reason) {
        try {
            InventoryItem updatedItem = inventoryService.updateStock(productId, quantity, reason);
            return ResponseEntity.ok(updatedItem);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }
    
    @PostMapping("/{productId}/reserve")
    public ResponseEntity<String> reserveStock(
            @PathVariable String productId,
            @RequestParam Integer quantity,
            @RequestParam String orderId) {
        boolean success = inventoryService.reserveStock(productId, quantity, orderId);
        if (success) {
            return ResponseEntity.ok("Stock reserved successfully");
        } else {
            return ResponseEntity.badRequest().body("Insufficient stock available");
        }
    }
    
    @PostMapping("/{productId}/release")
    public ResponseEntity<String> releaseStock(
            @PathVariable String productId,
            @RequestParam Integer quantity,
            @RequestParam String orderId) {
        inventoryService.releaseStock(productId, quantity, orderId);
        return ResponseEntity.ok("Stock released successfully");
    }
    
    @PostMapping("/{productId}/confirm")
    public ResponseEntity<String> confirmStockUsage(
            @PathVariable String productId,
            @RequestParam Integer quantity,
            @RequestParam String orderId) {
        inventoryService.confirmStockUsage(productId, quantity, orderId);
        return ResponseEntity.ok("Stock usage confirmed");
    }
    
    @PostMapping("/{productId}/add")
    public ResponseEntity<String> addStock(
            @PathVariable String productId,
            @RequestParam Integer quantity,
            @RequestParam(defaultValue = "Manual addition") String reason) {
        inventoryService.addStock(productId, quantity, reason);
        return ResponseEntity.ok("Stock added successfully");
    }
    
    @PostMapping("/{productId}/restock")
    public ResponseEntity<String> processRestock(
            @PathVariable String productId,
            @RequestParam Integer quantity) {
        inventoryService.processRestock(productId, quantity);
        return ResponseEntity.ok("Restock processed successfully");
    }
}
