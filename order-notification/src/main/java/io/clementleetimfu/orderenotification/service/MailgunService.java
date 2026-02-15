package io.clementleetimfu.orderenotification.service;

import io.clementleetimfu.ordercommon.event.OrderConfirmedEvent;
import io.clementleetimfu.ordercommon.event.OrderFailedEvent;

public interface MailgunService {

    boolean isAvailable();

    void sendOrderConfirmedEmail(OrderConfirmedEvent orderConfirmedEvent);

    void sendOrderFailedEmail(OrderFailedEvent orderFailedEvent);

}