# Kafka Order System

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.10-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.1-black.svg)](https://kafka.apache.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6%2B-red.svg)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](#license)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Kafka Topics](#kafka-topics)
- [Configuration](#configuration)
- [License](#license)

## Overview

A distributed event-driven order processing system built with Spring Boot and Apache Kafka. The system processes customer orders through a pipeline of microservices, implementing reliable messaging patterns including region-based partitioning, exponential backoff retry, and dead letter topic handling.

Orders flow through validation and notification stages, with automatic email notifications sent via Mailgun upon order confirmation or failure.

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│   Client    │────▶│    order-api    │────▶│ order-validation │────▶│ order-notification │
│   (REST)    │     │  (Port 8080)    │     │  (Port 8081)     │     │    (Port 8082)     │
└─────────────┘     └─────────────────┘     └──────────────────┘     └────────────────────┘
                           │                        │                        │
                           │                        │                        │
                           ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              Apache Kafka Cluster (3 Brokers)                           │
│                                                                                         │
│   Topics: order-placed ──▶ order-confirmed ──▶ Email (confirmed)                        │
│           order-placed ──▶ order-failed ──▶ Email (failed)                              │
│           order-placed-retry-0/1/2, order-placed-dlt (DLT for failed messages)          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

**Message Flow:**
1. **order-api** receives REST requests and publishes `OrderPlacedEvent` to Kafka
2. **order-validation** consumes orders, validates required fields, and emits `OrderConfirmedEvent` or `OrderFailedEvent`
3. **order-notification** consumes confirmed/failed events and sends transactional emails via Mailgun

## Features

- **Region-Based Partitioning** - Orders routed to specific partitions by region (ASIA→0, EUROPE→1, AMERICA→2)
- **Retry with Exponential Backoff** - 4 attempts with 2s→4s→8s delays using `@RetryableTopic`
- **Dead Letter Topic (DLT)** - Failed messages automatically routed to DLT for inspection
- **Transactional Emails** - Order confirmation and failure notifications via Mailgun
- **Audit Headers** - Correlation ID, timestamp, and source headers added via `ProducerInterceptor`
- **Consumer Metrics** - Message count, bytes consumed, and commit tracking via `ConsumerInterceptor` (order-validation)
- **Manual Acknowledgment** - Reliable message processing with explicit acknowledgment

## Technology Stack

| Category | Technology |
|----------|------------|
| Language | Java 17 |
| Framework | Spring Boot 3.5.10 |
| Messaging | Apache Kafka 4.1.1, Spring Kafka |
| Build Tool | Maven 3.6+ |
| Email Service | Mailgun API, Mailgun SDK 2.3 |
| Template Engine | Thymeleaf |
| Utilities | Hutool 5.8.43 |
| Boilerplate | Lombok |

## Project Structure

```
kafka-order-system/
├── order-common/                 # Shared DTOs, events, constants
│   └── src/main/java/io/clementleetimfu/ordercommon/
│       ├── constants/            # TopicConstants, RegionConstants, StatusConstants
│       │                         # GroupConstants, HeaderConstants, EmailConstants, OrderConstants
│       ├── dto/                  # OrderRequestDTO, OrderResponseDTO, OrderItemRequestDTO
│       └── event/                # OrderPlacedEvent, OrderConfirmedEvent, OrderFailedEvent
│                                 # OrderItem, OrderValidationResult
│
├── order-api/                    # REST API service (Port 8080)
│   └── src/main/java/io/clementleetimfu/orderapi/
│       ├── config/               # KafkaProducerConfig, TopicConfig
│       ├── controller/           # OrderController
│       ├── interceptor/          # AuditProducerInterceptor
│       ├── partitioner/          # RegionPartitioner
│       ├── producer/             # OrderProducer
│       └── service/              # OrderService, OrderServiceImpl
│
├── order-validation/             # Validation consumer (Port 8081)
│   └── src/main/java/io/clementleetimfu/ordervalidation/
│       ├── config/               # KafkaConsumerConfig
│       ├── consumer/             # OrderValidationConsumer
│       └── interceptor/          # MetricsConsumerInterceptor
│
├── order-notification/           # Email notification service (Port 8082)
│   └── src/main/
│       ├── java/io/clementleetimfu/orderenotification/
│       │   ├── config/               # KafkaConsumerConfig, MailgunConfig, MailgunProperties
│       │   ├── consumer/             # OrderNotificationConsumer
│       │   └── service/              # MailgunService, impl/MailgunServiceImpl
│       └── resources/
│           └── templates/email/      # order-confirmed.html, order-failed.html
│
└── docker/                       # Docker Compose for Kafka cluster
    └── docker-compose.yml
```

## Prerequisites

- **JDK 17** - OpenJDK or Oracle JDK
- **Maven 3.6+** - Build and dependency management
- **Apache Kafka Cluster** - 3-broker cluster (or use provided Docker Compose)
- **Mailgun Account** - API key and domain for email sending

## Getting Started

### Build

```bash
mvn clean compile                           # Compile all modules
cd order-common && mvn clean install -DskipTests  # Install common module first
mvn clean package -DskipTests               # Create jars without tests
```

### Run Services Locally

```bash
cd order-api && mvn spring-boot:run         # API on port 8080
cd order-validation && mvn spring-boot:run  # Validation on port 8081
cd order-notification && mvn spring-boot:run  # Notification on port 8082
```

## API Reference

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/orders/order` | Place order with region-based partitioning |
| `POST` | `/orders/order/default` | Place order using default topic |

### Request Body

```json
{
  "customerId": "CUSTOMER003",
  "region": "EUROPE",
  "items": [
    {
      "productId": "PRODUCT001",
      "productName": "Apple",
      "quantity": 2,
      "price": 29.99
    },
    {
      "productId": "PRODUCT002",
      "productName": "Orange",
      "quantity": 5,
      "price": 15.99
    },
    {
      "productId": "PRODUCT003",
      "productName": "Banana",
      "quantity": 3,
      "price": 79.99
    },
    {
      "productId": "PRODUCT004",
      "productName": "Pineapple",
      "quantity": 1,
      "price": 299.99
    }
  ],
  "priority": "HIGH",
  "email": "abc@example.com"
}
```

### Response

```json
{
  "orderId": "ORDER1739123456789abcdef123456",
  "status": "PLACED",
  "timestamp": "2026-02-15T10:30:00.123Z"
}
```

### Response Status Codes

| Status | Description |
|--------|-------------|
| `202 ACCEPTED` | Order accepted and sent to Kafka |
| `500 INTERNAL_SERVER_ERROR` | Failed to process order |

## Kafka Topics

### Main Topics

| Topic | Partitions | Producer | Consumer | Purpose |
|-------|------------|----------|----------|---------|
| `order-placed` | 3 | order-api | order-validation | Incoming orders from API |
| `order-confirmed` | 3 | order-validation | order-notification | Successfully validated orders |
| `order-failed` | 3 | order-validation | order-notification | Orders that failed validation |

### Retry Topics (Auto-created by @RetryableTopic)

| Topic | Delay | Purpose |
|-------|-------|---------|
| `order-placed-retry-0` | 2s | First retry attempt |
| `order-placed-retry-1` | 4s | Second retry attempt |
| `order-placed-retry-2` | 8s | Third retry attempt |
| `order-confirmed-retry-0/1/2` | 2s→4s→8s | Notification retries |
| `order-failed-retry-0/1/2` | 2s→4s→8s | Notification retries |

All retry topics use exponential backoff: 2s → 4s → 8s (max 16s), with 4 total attempts (1 original + 3 retries).

### Dead Letter Topics (Auto-created by @RetryableTopic)

| Topic | Purpose |
|-------|---------|
| `order-placed-dlt` | Validation messages that failed after all retries |
| `order-confirmed-dlt` | Notification messages that failed after all retries |
| `order-failed-dlt` | Notification messages that failed after all retries |

### Region Partitioning

| Region | Partition | Route |
|--------|-----------|-------|
| `ASIA` | 0 | Asia-Pacific customers |
| `EUROPE` | 1 | European customers |
| `AMERICA` | 2 | Americas customers |

### Consumer Groups

| Group ID | Service | Purpose |
|----------|---------|---------|
| `order-validation-group` | order-validation | Consumes from `order-placed` |
| `order-email-group` | order-notification | Consumes from `order-confirmed`, `order-failed` |

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MAILGUN_API_KEY` | Yes | Mailgun API key for sending emails |
| `MAILGUN_DOMAIN` | Yes | Mailgun domain for email sending |

### Application Properties

Each service has its own `application.yml`:

| Service | Property | Default Value |
|---------|----------|---------------|
| order-api | `server.port` | 8080 |
| order-api | `spring.application.name` | order-api |
| order-validation | `server.port` | 8081 |
| order-validation | `spring.application.name` | order-validation |
| order-notification | `server.port` | 8082 |
| order-notification | `spring.application.name` | order-notification-service |
| All | `spring.kafka.bootstrap-servers` | 192.168.100.131:9092,9093,9094 |
| order-notification | `mailgun.from` | noreply@kafka-order-system.com |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.