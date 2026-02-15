package io.clementleetimfu.orderapi.service.impl;

import cn.hutool.core.bean.BeanUtil;
import io.clementleetimfu.ordercommon.constants.OrderConstants;
import io.clementleetimfu.ordercommon.constants.StatusConstants;
import io.clementleetimfu.ordercommon.dto.OrderItemRequestDTO;
import io.clementleetimfu.ordercommon.dto.OrderRequestDTO;
import io.clementleetimfu.ordercommon.dto.OrderResponseDTO;
import io.clementleetimfu.ordercommon.event.OrderItem;
import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import io.clementleetimfu.orderapi.producer.OrderProducer;
import io.clementleetimfu.orderapi.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Slf4j
@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    private OrderProducer orderProducer;

    public ResponseEntity<OrderResponseDTO> placeOrder(OrderRequestDTO orderRequestDTO) {
        return processOrder(orderRequestDTO, orderProducer::sendOrder);
    }

    public ResponseEntity<OrderResponseDTO> placeOrderDefault(OrderRequestDTO orderRequestDTO) {
        return processOrder(orderRequestDTO, orderProducer::sendOrderDefault);
    }

    private ResponseEntity<OrderResponseDTO> processOrder(
            OrderRequestDTO orderRequestDTO,
            Function<OrderPlacedEvent, CompletableFuture<SendResult<String, OrderPlacedEvent>>> sendFunction) {

        String orderId = generateOrderId();
        List<OrderItem> orderItems = toOrderItemList(orderRequestDTO.getItems());
        OrderPlacedEvent orderPlacedEvent = buildOrderPlacedEvent(orderId, orderRequestDTO, orderItems);

        try {
            sendFunction.apply(orderPlacedEvent);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(buildAcceptedResponse(orderPlacedEvent));

        } catch (Exception e) {
            log.error("Failed to place order {}", orderId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    private String generateOrderId() {
        return OrderConstants.ORDER_PREFIX +
                System.currentTimeMillis() +
                UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private List<OrderItem> toOrderItemList(List<OrderItemRequestDTO> items) {
        return BeanUtil.copyToList(items, OrderItem.class);
    }

    private OrderPlacedEvent buildOrderPlacedEvent(String orderId, OrderRequestDTO orderRequestDTO, List<OrderItem> orderItems) {
        return OrderPlacedEvent.builder()
                .orderId(orderId)
                .customerId(orderRequestDTO.getCustomerId())
                .email(orderRequestDTO.getEmail())
                .region(orderRequestDTO.getRegion())
                .items(orderItems)
                .priority(orderRequestDTO.getPriority())
                .totalAmount(calculateTotal(orderRequestDTO.getItems()))
                .status(StatusConstants.PLACED)
                .placedAt(Instant.now())
                .build();
    }

    private OrderResponseDTO buildAcceptedResponse(OrderPlacedEvent orderPlacedEvent) {
        return OrderResponseDTO.builder()
                .orderId(orderPlacedEvent.getOrderId())
                .status(orderPlacedEvent.getStatus())
                .timestamp(orderPlacedEvent.getPlacedAt().toString())
                .build();
    }

    private BigDecimal calculateTotal(List<OrderItemRequestDTO> items) {
        return items.stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
