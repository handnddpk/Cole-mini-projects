package com.realtime.api.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
@EnableScheduling
public class ApiDataCollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiDataCollectorApplication.class, args);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        
        return new KafkaProducer<>(props);
    }
}

@Service
class FinancialDataCollector {
    
    private static final Logger logger = LoggerFactory.getLogger(FinancialDataCollector.class);
    
    @Value("${api.alphavantage.key:DEMO_KEY}")
    private String apiKey;
    
    @Value("${api.alphavantage.symbols:AAPL,GOOGL,MSFT,TSLA,AMZN}")
    private String symbols;
    
    private final RestTemplate restTemplate;
    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    
    public FinancialDataCollector(RestTemplate restTemplate, 
                                 KafkaProducer<String, String> kafkaProducer,
                                 ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.executorService = Executors.newFixedThreadPool(5);
    }
    
    @Scheduled(fixedRate = 60000) // Every minute
    public void collectStockPrices() {
        String[] symbolArray = symbols.split(",");
        
        for (String symbol : symbolArray) {
            executorService.submit(() -> fetchStockData(symbol.trim()));
        }
    }
    
    private void fetchStockData(String symbol) {
        try {
            String url = String.format(
                "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=%s&apikey=%s",
                symbol, apiKey
            );
            
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = objectMapper.readTree(response.getBody());
                JsonNode quote = jsonNode.get("Global Quote");
                
                if (quote != null) {
                    Map<String, Object> stockData = new HashMap<>();
                    stockData.put("symbol", quote.get("01. symbol").asText());
                    stockData.put("price", quote.get("05. price").asDouble());
                    stockData.put("change", quote.get("09. change").asDouble());
                    stockData.put("changePercent", quote.get("10. change percent").asText());
                    stockData.put("volume", quote.get("06. volume").asLong());
                    stockData.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    stockData.put("source", "alphavantage");
                    
                    String message = objectMapper.writeValueAsString(stockData);
                    
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        "financial-data", symbol, message);
                    
                    kafkaProducer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            logger.info("Financial data sent for {}: partition={}, offset={}",
                                       symbol, metadata.partition(), metadata.offset());
                        } else {
                            logger.error("Failed to send financial data for {}", symbol, exception);
                        }
                    });
                }
            }
            
        } catch (Exception e) {
            logger.error("Error fetching stock data for {}", symbol, e);
        }
    }
}

@Service 
class WeatherDataCollector {
    
    private static final Logger logger = LoggerFactory.getLogger(WeatherDataCollector.class);
    
    @Value("${api.openweather.key:DEMO_KEY}")
    private String apiKey;
    
    @Value("${api.openweather.cities:London,NewYork,Tokyo,Sydney,Mumbai}")
    private String cities;
    
    private final RestTemplate restTemplate;
    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper;
    
    public WeatherDataCollector(RestTemplate restTemplate,
                               KafkaProducer<String, String> kafkaProducer,
                               ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
    }
    
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void collectWeatherData() {
        String[] cityArray = cities.split(",");
        
        for (String city : cityArray) {
            fetchWeatherData(city.trim());
        }
    }
    
    private void fetchWeatherData(String city) {
        try {
            String url = String.format(
                "https://api.openweathermap.org/data/2.5/weather?q=%s&appid=%s&units=metric",
                city, apiKey
            );
            
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = objectMapper.readTree(response.getBody());
                
                Map<String, Object> weatherData = new HashMap<>();
                weatherData.put("city", city);
                weatherData.put("country", jsonNode.get("sys").get("country").asText());
                weatherData.put("temperature", jsonNode.get("main").get("temp").asDouble());
                weatherData.put("humidity", jsonNode.get("main").get("humidity").asInt());
                weatherData.put("pressure", jsonNode.get("main").get("pressure").asInt());
                weatherData.put("description", jsonNode.get("weather").get(0).get("description").asText());
                weatherData.put("windSpeed", jsonNode.get("wind").get("speed").asDouble());
                weatherData.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                weatherData.put("source", "openweathermap");
                
                String message = objectMapper.writeValueAsString(weatherData);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "weather-data", city, message);
                
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Weather data sent for {}: partition={}, offset={}",
                                   city, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to send weather data for {}", city, exception);
                    }
                });
            }
            
        } catch (Exception e) {
            logger.error("Error fetching weather data for {}", city, e);
        }
    }
}

@Service
class EcommerceDataSimulator {
    
    private static final Logger logger = LoggerFactory.getLogger(EcommerceDataSimulator.class);
    
    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper;
    
    private final String[] products = {"Laptop", "Phone", "Tablet", "Headphones", "Watch", "Camera"};
    private final String[] customers = {"customer-001", "customer-002", "customer-003", "customer-004", "customer-005"};
    
    public EcommerceDataSimulator(KafkaProducer<String, String> kafkaProducer,
                                 ObjectMapper objectMapper) {
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
    }
    
    @Scheduled(fixedRate = 10000) // Every 10 seconds
    public void generateOrderEvents() {
        try {
            Map<String, Object> orderData = new HashMap<>();
            orderData.put("orderId", "order-" + System.currentTimeMillis());
            orderData.put("customerId", customers[(int) (Math.random() * customers.length)]);
            orderData.put("productName", products[(int) (Math.random() * products.length)]);
            orderData.put("quantity", (int) (Math.random() * 5) + 1);
            orderData.put("price", Math.round((Math.random() * 1000 + 100) * 100.0) / 100.0);
            orderData.put("status", "PENDING");
            orderData.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            orderData.put("source", "ecommerce-simulator");
            
            String message = objectMapper.writeValueAsString(orderData);
            
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "ecommerce-orders", orderData.get("orderId").toString(), message);
            
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Order event sent: {}: partition={}, offset={}",
                               orderData.get("orderId"), metadata.partition(), metadata.offset());
                } else {
                    logger.error("Failed to send order event: {}", orderData.get("orderId"), exception);
                }
            });
            
        } catch (Exception e) {
            logger.error("Error generating order event", e);
        }
    }
}
