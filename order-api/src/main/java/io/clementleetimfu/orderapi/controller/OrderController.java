package io.clementleetimfu.orderapi.controller;

import io.clementleetimfu.ordercommon.dto.OrderRequestDTO;
import io.clementleetimfu.ordercommon.dto.OrderResponseDTO;
import io.clementleetimfu.orderapi.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping("/order")
    public ResponseEntity<OrderResponseDTO> placeOrder(@RequestBody OrderRequestDTO orderRequestDTO) {
        return orderService.placeOrder(orderRequestDTO);
    }

    @PostMapping("/order/default")
    public ResponseEntity<OrderResponseDTO> placeOrderDefault(@RequestBody OrderRequestDTO orderRequestDTO) {
        return orderService.placeOrderDefault(orderRequestDTO);
    }

}