package io.clementleetimfu.ordercommon.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderFailedEvent implements OrderValidationResult {

    private String orderId;

    private String customerId;

    private String email;

    private String region;

    private String status;

    private List<String> failureReasons;

    private Instant failedAt;
}