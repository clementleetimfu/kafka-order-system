# Repository Guidelines

## Project Structure & Module Organization
Java 17 multi-module Maven project for Kafka-based order processing using Spring Boot 3.5.10.

Root modules:
- `order-common`: Shared DTOs, events, constants
- `order-api`: REST API with Kafka producer (port 8080)
- `order-validation`: Kafka consumer for order validation (port 8081)
- `order-notification`: Kafka consumer for email notifications via Mailgun (port 8082)

Package structure: `io.clementleetimfu.<module>` (e.g., `ordercommon`, `orderapi`, `ordervalidation`)

```
order-common/src/main/java/
  ├── constants/     # TopicConstants, RegionConstants, StatusConstants
  ├── dto/           # OrderRequestDTO, OrderResponseDTO, OrderItemRequestDTO
  └── event/         # OrderPlacedEvent, OrderConfirmedEvent, OrderFailedEvent

*/src/main/java/
  ├── config/        # KafkaProducerConfig, KafkaConsumerConfig, MailgunConfig
  ├── controller/    # REST controllers (order-api only)
  ├── service/      # Service interfaces and impl/ implementations
  ├── producer/      # Kafka producers
  ├── consumer/      # Kafka consumers
  ├── interceptor/   # Kafka producer/consumer interceptors
  └── partitioner/   # Custom Kafka partitioners
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

Requirements: Java 17, Maven 3.6+, Kafka cluster

## Coding Style & Naming Conventions

### Imports
Order: third-party (Lombok, Spring, Hutool), project packages, JDK. No wildcard imports except static constants.

### Class Naming
- Classes: `PascalCase` (e.g., `OrderProducer`, `RegionPartitioner`)
- DTOs: suffix with `DTO` (e.g., `OrderRequestDTO`)
- Events: suffix with `Event` (e.g., `OrderPlacedEvent`)
- Constants classes: suffix with `Constants` (e.g., `TopicConstants`)
- Tests: suffix with `Test` (unit tests), `IT` (integration tests)

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
Use `@Slf4j` for logger injection.

### Dependency Injection
Use `@Autowired` for field injection (current pattern). Constructor injection via `@RequiredArgsConstructor` also acceptable.

### Constants
All constants in `order-common`. Use `public static final` with private constructor:
```java
public final class TopicConstants {
    public static final String ORDER_PLACED = "order-placed";
    private TopicConstants() {}
}
```
Never hardcode topic names, status values, or configuration strings.

### Error Handling
- Use `log.error()` with message and exception for error logging
- Wrap exceptions with context: `throw new RuntimeException("Order processing failed: " + orderId, e);`
- Return appropriate HTTP status codes (HttpStatus.ACCEPTED, HttpStatus.INTERNAL_SERVER_ERROR)

### Logging
- Log levels: DEBUG for detailed flow, INFO for key events, ERROR for failures
- Include relevant IDs: `log.info("Order sent: orderId={}", orderId)`

### Java 17 Features
- Switch expressions: `return switch (region) { case "ASIA" -> 0; case "EUROPE" -> 1; default -> 2; };`
- Pattern matching: `if (value instanceof OrderPlacedEvent event) { return event.getRegion(); }`

### Hutool Library
Use Hutool utilities for common operations:
- `BeanUtil.copyToList()` for bean mapping
- `StrUtil.isBlank()` for string null/empty checks
- `CollectionUtil.isEmpty()` for collection null/empty checks

### Kafka Patterns
- **Producers**: Use `CompletableFuture` with `thenAccept`/`exceptionally` callbacks
- **Consumers**: Manual acknowledgment (`Acknowledgment acknowledgment.acknowledge()`)
- **Retry**: `@RetryableTopic` with `@Backoff` for exponential backoff
- **DLT**: `@DltHandler` method for dead letter topic processing
- **Partitioners**: Implement `Partitioner` interface for custom routing
- **Interceptors**: Implement `ProducerInterceptor`/`ConsumerInterceptor` for cross-cutting concerns

### Service Implementation Pattern
```java
public interface OrderService {
    ResponseEntity<OrderResponseDTO> placeOrder(OrderRequestDTO dto);
}

@Slf4j
@Service
public class OrderServiceImpl implements OrderService {
    @Autowired
    private OrderProducer orderProducer;
    
    @Override
    public ResponseEntity<OrderResponseDTO> placeOrder(OrderRequestDTO dto) {
        // Implementation
    }
}
```

## Testing Guidelines

Spring Boot test support available (`spring-boot-starter-test`, `spring-kafka-test`).

- Place tests in `src/test/java` mirroring production packages
- Use `@SpringBootTest` for integration tests, `@EmbeddedKafka` for Kafka tests
- Run `mvn test` before PRs

## Commit & Pull Request Guidelines

Commit messages: short, imperative, module-scoped:
- `order-api: add order cancellation endpoint`
- `order-common: add PaymentStatus constants`
- `order-validation: fix retry backoff configuration`

PRs should include: purpose, modules changed, test evidence, config changes, backward-compatibility impact for event/schema changes.