package com.ecommerce.inventory.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.ecommerce.inventory.model.InventoryItem;

@Repository
public interface InventoryRepository extends JpaRepository<InventoryItem, Long> {
    
    Optional<InventoryItem> findByProductId(String productId);
    
    List<InventoryItem> findByCategory(String category);
    
    @Query("SELECT i FROM InventoryItem i WHERE i.quantity - i.reservedQuantity <= i.reorderThreshold")
    List<InventoryItem> findLowStockItems();
    
    @Query("SELECT i FROM InventoryItem i WHERE i.quantity - i.reservedQuantity >= :requiredQuantity AND i.productId = :productId")
    Optional<InventoryItem> findAvailableItem(@Param("productId") String productId, @Param("requiredQuantity") Integer requiredQuantity);
    
    List<InventoryItem> findBySupplier(String supplier);
    
    List<InventoryItem> findByLocation(String location);
    
    @Query("SELECT i FROM InventoryItem i WHERE i.productName LIKE %:searchTerm% OR i.category LIKE %:searchTerm%")
    List<InventoryItem> searchByNameOrCategory(@Param("searchTerm") String searchTerm);
}
