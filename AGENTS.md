# Repository Guidelines

## Project Structure & Module Organization
This is a Java 17 multi-module Maven project for Kafka-based order processing. Spring Boot 3.5.10 is used.

Root modules (declared in `pom.xml`):
- `order-common`: Shared DTOs, events, constants, enums
- `order-api`: REST API with Kafka producer
- `order-validation`: Kafka consumer for order validation
- `order-notification`: Kafka consumer for email notifications via Mailgun

Package structure:
- `io.clementleetimfu.ordercommon` (order-common)
- `io.clementleetimfu.orderapi` (order-api)
- `io.clementleetimfu.ordervalidation` (order-validation)
- `io.clementleetimfu.orderenotification` (order-notification)

```
order-common/src/main/java/
  ├── constants/     # TopicConstants, RegionConstants, StatusConstants, etc.
  ├── dto/           # OrderRequestDTO, OrderResponseDTO, OrderItemRequestDTO
  └── event/         # OrderPlacedEvent, OrderConfirmedEvent, OrderFailedEvent, OrderItem

*/src/main/java/
  ├── config/        # KafkaProducerConfig, KafkaConsumerConfig, MailgunConfig, etc.
  ├── controller/    # REST controllers (order-api only)
  ├── service/       # Service interfaces and impl/ implementations
  ├── producer/      # Kafka producers
  ├── consumer/      # Kafka consumers
  ├── interceptor/   # Kafka producer/consumer interceptors
  └── partitioner/   # Custom Kafka partitioners

*/src/main/resources/
  └── application.yml  # Service configuration
```

## Build, Test, and Development Commands

### Build Commands
```bash
mvn clean compile                           # Compile all modules from root
cd order-common && mvn clean install -DskipTests  # Install common module first
mvn clean package -DskipTests               # Create jars without tests
```

### Test Commands
```bash
mvn test                                    # Run all tests across all modules
mvn test -pl order-api                      # Run tests for specific module
mvn test -pl order-api -Dtest=OrderServiceTest  # Run single test class
mvn test -Dtest=OrderServiceTest#testPlaceOrder  # Run single test method
mvn test -pl order-api,order-validation     # Run tests for multiple modules
```

### Run Services Locally
```bash
cd order-api && mvn spring-boot:run         # API on port 8080
cd order-validation && mvn spring-boot:run  # Validation on port 8081
cd order-notification && mvn spring-boot:run  # Notification on port 8082
```

### Requirements
- Java 17
- Maven 3.6+
- Kafka cluster (bootstrap servers configured in application.yml)

## Coding Style & Naming Conventions

### Imports
Group imports in order: JDK, third-party, project packages. Use wildcard imports only for static constants.

### Class Naming
- Classes: `PascalCase` (e.g., `OrderProducer`, `RegionPartitioner`, `AuditProducerInterceptor`)
- DTOs: suffix with `DTO` (e.g., `OrderRequestDTO`, `OrderResponseDTO`)
- Events: suffix with `Event` (e.g., `OrderPlacedEvent`, `OrderConfirmedEvent`)
- Constants classes: suffix with `Constants` (e.g., `TopicConstants`, `StatusConstants`)
- Tests: suffix with `Test` for unit tests, `IT` for integration tests

### Package Naming
Use singular nouns: `controller`, `service`, `consumer`, `producer`, `config`, `interceptor`, `partitioner`.

### Lombok Patterns
Use Lombok annotations consistently:
```java
@Data                           // Getters, setters, equals, hashCode, toString
@AllArgsConstructor             // All-args constructor
@NoArgsConstructor              // No-args constructor
@Builder                        // Builder pattern for events/DTOs
@Slf4j                          // Logger injection
```

Example:
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderPlacedEvent {
    private String orderId;
    private Instant placedAt;
}
```

### Dependency Injection
Use `@Autowired` for field injection (current pattern in codebase). Constructor injection via `@RequiredArgsConstructor` is also acceptable.

### Constants
All constants centralized in `order-common`. Use `public static final` with private constructor:

```java
public final class TopicConstants {
    public static final String ORDER_PLACED = "order-placed";
    private TopicConstants() {}
}
```

Never hardcode topic names, status values, or configuration strings in service code.

### Error Handling
- Use `log.error()` with message and exception for error logging
- Wrap exceptions with context before re-throwing:
```java
catch (Exception e) {
    log.error("Failed to process order {}", orderId, e);
    throw new RuntimeException("Order processing failed: " + orderId, e);
}
```
- Return appropriate HTTP status codes in controllers (HttpStatus.ACCEPTED, HttpStatus.INTERNAL_SERVER_ERROR)

### Logging
- Use `@Slf4j` annotation for logger injection
- Log levels: DEBUG for detailed flow, INFO for key events, ERROR for failures
- Include relevant IDs in log messages: `log.info("Order sent: orderId={}", orderId)`

### Java 17 Features
Utilize modern Java features:
- Switch expressions: `return switch (region) { case "ASIA" -> 0; case "EUROPE" -> 1; default -> 2; };`
- Pattern matching: `if (value instanceof OrderPlacedEvent event) { return event.getRegion(); }`
- Text blocks for multi-line strings (when needed)

### Kafka Patterns
- **Producers**: Use `CompletableFuture` with callbacks for async send operations
- **Consumers**: Use manual acknowledgment (`ContainerProperties.AckMode.MANUAL`)
- **Retry**: Use `@RetryableTopic` with exponential backoff for consumer retries
- **DLT**: Implement `@DltHandler` for dead letter topic processing
- **Partitioners**: Implement `Partitioner` interface for custom routing
- **Interceptors**: Implement `ProducerInterceptor`/`ConsumerInterceptor` for cross-cutting concerns

### Service Implementation Pattern
Create interface and impl class:
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

Spring Boot test support is available (`spring-boot-starter-test`, `spring-kafka-test`).

- Place tests in `src/test/java` mirroring production packages
- Name unit tests `*Test` and integration tests `*IT`
- Use `@SpringBootTest` for integration tests
- Use `@EmbeddedKafka` for Kafka integration tests
- Run module-level tests before PRs: `cd <module> && mvn test`

## Commit & Pull Request Guidelines

Commit messages should be short, imperative, and module-scoped:
- `order-api: add order cancellation endpoint`
- `order-common: add PaymentStatus constants`
- `order-validation: fix retry backoff configuration`

PRs should include:
- Purpose and modules changed
- Test evidence (`mvn test` output summary)
- Configuration/environment changes
- For event/schema changes: affected topics and backward-compatibility impact