package io.clementleetimfu.ordercommon.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderItem {

    private String productId;

    private String productName;

    private Integer quantity;

    private BigDecimal price;
}