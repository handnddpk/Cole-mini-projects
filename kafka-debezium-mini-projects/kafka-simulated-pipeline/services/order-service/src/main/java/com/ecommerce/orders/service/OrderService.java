package com.ecommerce.orders.service;

import com.ecommerce.orders.events.OrderEvent;
import com.ecommerce.orders.events.OrderEventPublisher;
import com.ecommerce.orders.model.Order;
import com.ecommerce.orders.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class OrderService {
    
    private final AtomicLong orderIdGenerator = new AtomicLong(1);
    
    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private OrderEventPublisher eventPublisher;
    
    public Order createOrder(Long customerId, Long productId, Integer quantity, BigDecimal price) {
        // Create new order
        Order order = new Order(customerId, productId, quantity, price);
        order.setId(orderIdGenerator.getAndIncrement());
        
        // Save to repository
        orderRepository.save(order);
        
        // Publish OrderCreated event
        OrderEvent event = new OrderEvent(
            "OrderCreated",
            order.getId(),
            order.getCustomerId(),
            order.getProductId(),
            order.getQuantity(),
            order.getPrice(),
            order.getTotalAmount(),
            order.getStatus().toString(),
            System.currentTimeMillis()
        );
        
        eventPublisher.publishOrderEvent(event);
        
        return order;
    }
    
    public Order getOrder(Long orderId) {
        return orderRepository.findById(orderId);
    }
    
    public List<Order> getOrdersByCustomer(Long customerId) {
        return orderRepository.findByCustomerId(customerId);
    }
    
    public Order updateOrderStatus(Long orderId, String status) {
        Order order = orderRepository.findById(orderId);
        if (order == null) {
            return null;
        }
        
        String oldStatus = order.getStatus().toString();
        order.setStatus(OrderStatus.valueOf(status.toUpperCase()));
        
        // Save updated order
        orderRepository.save(order);
        
        // Publish OrderStatusUpdated event
        OrderEvent event = new OrderEvent(
            "OrderStatusUpdated",
            order.getId(),
            order.getCustomerId(),
            order.getProductId(),
            order.getQuantity(),
            order.getPrice(),
            order.getTotalAmount(),
            order.getStatus().toString(),
            System.currentTimeMillis()
        );
        
        eventPublisher.publishOrderEvent(event);
        
        return order;
    }
    
    public void cancelOrder(Long orderId) {
        Order order = orderRepository.findById(orderId);
        if (order == null) {
            throw new RuntimeException("Order not found: " + orderId);
        }
        
        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);
        
        // Publish OrderCancelled event
        OrderEvent event = new OrderEvent(
            "OrderCancelled",
            order.getId(),
            order.getCustomerId(),
            order.getProductId(),
            order.getQuantity(),
            order.getPrice(),
            order.getTotalAmount(),
            order.getStatus().toString(),
            System.currentTimeMillis()
        );
        
        eventPublisher.publishOrderEvent(event);
    }
}
