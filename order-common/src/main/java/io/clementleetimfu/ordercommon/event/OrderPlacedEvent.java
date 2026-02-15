package io.clementleetimfu.ordercommon.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderPlacedEvent {

    private String orderId;

    private String customerId;

    private String email;

    private String region;

    private List<OrderItem> items;

    private String priority;

    private BigDecimal totalAmount;

    private String status;

    private Instant placedAt;
}