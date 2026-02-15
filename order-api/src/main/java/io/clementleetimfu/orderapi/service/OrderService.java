package io.clementleetimfu.orderapi.service;

import io.clementleetimfu.ordercommon.dto.OrderRequestDTO;
import io.clementleetimfu.ordercommon.dto.OrderResponseDTO;
import org.springframework.http.ResponseEntity;

public interface OrderService {

    ResponseEntity<OrderResponseDTO> placeOrder(OrderRequestDTO orderRequestDTO);

    ResponseEntity<OrderResponseDTO> placeOrderDefault(OrderRequestDTO orderRequestDTO);

}