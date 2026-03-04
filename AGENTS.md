# Repository Guidelines

## Project Structure & Module Organization
Java 17 multi-module Maven project for Kafka-based order processing using Spring Boot 3.5.10.

Root modules:
- `order-common`: Shared DTOs, events, constants
- `order-api`: REST API with Kafka producer (port 8080)
- `order-validation`: Kafka consumer for order validation (port 8081)
- `order-notification`: Kafka consumer for email notifications via Mailgun (port 8082)

Package structure: `io.clementleetimfu.<module>` (e.g., `ordercommon`, `orderapi`, `ordervalidation`, `orderenotification`)

```
order-common/src/main/java/
  ├── constants/     # TopicConstants, RegionConstants, StatusConstants, GroupConstants
  ├── dto/           # OrderRequestDTO, OrderResponseDTO, OrderItemRequestDTO
  └── event/         # OrderPlacedEvent, OrderConfirmedEvent, OrderFailedEvent, OrderItem

*/src/main/java/
  ├── config/        # KafkaProducerConfig, KafkaConsumerConfig, MailgunConfig
  ├── controller/    # REST controllers (order-api only)
  ├── service/       # Service interfaces and impl/ implementations
  ├── producer/      # Kafka producers
  ├── consumer/      # Kafka consumers
  └── interceptor/   # Kafka producer/consumer interceptors
```

## Build, Test, and Development Commands

```bash
# Build commands
mvn clean compile                              # Compile all modules
cd order-common && mvn clean install -DskipTests  # Install common module first
mvn clean package -DskipTests                  # Create jars without tests

# Test commands
mvn test                                       # Run all tests across all modules
mvn test -pl order-api                         # Run tests for specific module
mvn test -pl order-api -Dtest=OrderServiceTest # Run single test class
mvn test -Dtest=OrderServiceTest#testPlaceOrder # Run single test method
mvn test -pl order-api,order-validation        # Run tests for multiple modules

# Run services locally
cd order-api && mvn spring-boot:run           # API on port 8080
cd order-validation && mvn spring-boot:run     # Validation on port 8081
cd order-notification && mvn spring-boot:run   # Notification on port 8082
```

Requirements: Java 17, Maven 3.6+, Kafka cluster (3 brokers via Docker Compose)

## Coding Style & Naming Conventions

### Imports
Order: Lombok, Spring/Hutool, project packages, JDK. No wildcard imports except static constants.

Example:
```java
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.clementleetimfu.ordercommon.constants.TopicConstants;
import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import java.time.Instant;
import java.util.List;
```

### Class Naming
- Classes: `PascalCase` (e.g., `OrderProducer`, `RegionPartitioner`)
- DTOs: suffix with `DTO` (e.g., `OrderRequestDTO`)
- Events: suffix with `Event` (e.g., `OrderPlacedEvent`)
- Constants classes: suffix with `Constants` (e.g., `TopicConstants`)
- Tests: suffix with `Test` (unit tests), `IT` (integration tests)
- Interfaces: no prefix/suffix (e.g., `OrderService`, `MailgunService`)
- Implementations: suffix with `Impl` (e.g., `OrderServiceImpl`)

### Package Naming
Use singular nouns: `controller`, `service`, `consumer`, `producer`, `config`, `interceptor`, `partitioner`.

### Lombok Patterns
```java
@Data @AllArgsConstructor @NoArgsConstructor @Builder
public class OrderPlacedEvent {
    private String orderId;
    private Instant placedAt;
}
```
Use `@Slf4j` for logger injection. Group annotations on single line when using multiple.

### Dependency Injection
Use `@Autowired` for field injection (current pattern). Constructor injection via `@RequiredArgsConstructor` also acceptable.

### Constants
All constants in `order-common` module. Use `public static final` with private constructor:
```java
public final class TopicConstants {
    public static final String ORDER_PLACED = "order-placed";
    public static final int PARTITIONS = 3;
    private TopicConstants() {}
}
```
Never hardcode topic names, status values, region strings, or configuration strings.

### Error Handling
- Use `log.error()` with message and exception for error logging
- Wrap exceptions with context: `throw new RuntimeException("Order processing failed: " + orderId, e);`
- Return appropriate HTTP status codes (`HttpStatus.ACCEPTED`, `HttpStatus.INTERNAL_SERVER_ERROR`)
- Always include relevant IDs in log messages

### Logging
- Log levels: DEBUG for detailed flow, INFO for key events, ERROR for failures
- Include relevant IDs: `log.info("Order sent: orderId={}", orderId)`
- Use parameterized logging, not string concatenation

### Java 17 Features
- Switch expressions: `return switch (region) { case RegionConstants.ASIA -> 0; case RegionConstants.EUROPE -> 1; default -> fallbackPartition(keyBytes, numPartitions); };`
- Pattern matching for `instanceof`:
```java
if (value instanceof OrderPlacedEvent event) {
    return event.getRegion();
}
if (orderValidationResult instanceof OrderConfirmedEvent confirmedEvent) {
    orderId = confirmedEvent.getOrderId();
}
```

### Hutool Library
Use Hutool utilities for common operations:
- `BeanUtil.copyToList()` for bean mapping
- `StrUtil.isBlank()` for string null/empty checks
- `CollectionUtil.isEmpty()` for collection null/empty checks

### Kafka Patterns

#### Producer Pattern
```java
public CompletableFuture<SendResult<String, OrderPlacedEvent>> sendOrder(OrderPlacedEvent event) {
    CompletableFuture<SendResult<String, OrderPlacedEvent>> future =
            kafkaTemplate.send(TopicConstants.ORDER_PLACED, event.getOrderId(), event);
    attachCallbacks(future, event);
    return future;
}

private void attachCallbacks(CompletableFuture<...> future, OrderPlacedEvent event) {
    future.thenAccept(result -> {
        log.debug("Order sent: topic={}, partition={}, offset={}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }).exceptionally(e -> {
        log.error("Failed to send order {}", event.getOrderId(), e);
        return null;
    });
}
```

#### Consumer Pattern
```java
@RetryableTopic(
    attempts = "4",
    backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 16000),
    autoCreateTopics = "true",
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
    dltStrategy = DltStrategy.FAIL_ON_ERROR,
    include = {Exception.class}
)
@KafkaListener(topics = TopicConstants.ORDER_PLACED, groupId = GroupConstants.VALIDATION)
public void onOrderPlaced(ConsumerRecord<String, OrderPlacedEvent> record, Acknowledgment ack) {
    // Process message
    ack.acknowledge();
}

@DltHandler
public void handleDlt(ConsumerRecord<String, OrderPlacedEvent> record, Acknowledgment ack) {
    log.error("DLT event: topic={}, value={}", record.topic(), record.value());
    ack.acknowledge();
}
```

#### Custom Partitioner
Implement `Partitioner` interface:
```java
@Override
public int partition(String topic, Object key, byte[] keyBytes, Object value, 
                      byte[] valueBytes, Cluster cluster) {
    return switch (region) {
        case RegionConstants.ASIA -> 0;
        case RegionConstants.EUROPE -> 1;
        case RegionConstants.AMERICA -> 2;
        default -> fallbackPartition(keyBytes, numPartitions);
    };
}
```

#### Interceptor Pattern
ProducerInterceptor adds audit headers; ConsumerInterceptor tracks metrics.

## Testing Guidelines

Spring Boot test support available (`spring-boot-starter-test`, `spring-kafka-test`).

- Place tests in `src/test/java` mirroring production packages
- Use `@SpringBootTest` for integration tests, `@EmbeddedKafka` for Kafka tests
- No wildcard imports except static constants
- Run `mvn test` before PRs

## Commit & Pull Request Guidelines

Commit messages: short, imperative, module-scoped:
- `order-api: add order cancellation endpoint`
- `order-common: add PaymentStatus constants`
- `order-validation: fix retry backoff configuration`

PRs should include: purpose, modules changed, test evidence, config changes, backward-compatibility impact for event/schema changes.