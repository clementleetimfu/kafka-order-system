package io.clementleetimfu.ordercommon.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderRequestDTO {

    private String customerId;

    private String email;

    private String region;

    private List<OrderItemRequestDTO> items;

    private String priority;
}