package io.clementleetimfu.orderenotification.service.impl;

import cn.hutool.core.util.StrUtil;
import com.mailgun.api.v3.MailgunMessagesApi;
import com.mailgun.model.message.Message;
import com.mailgun.model.message.MessageResponse;
import io.clementleetimfu.ordercommon.constants.EmailConstants;
import io.clementleetimfu.ordercommon.constants.TopicConstants;
import io.clementleetimfu.ordercommon.event.OrderConfirmedEvent;
import io.clementleetimfu.ordercommon.event.OrderFailedEvent;
import io.clementleetimfu.orderenotification.config.MailgunProperties;
import io.clementleetimfu.orderenotification.service.MailgunService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.time.Year;

@Slf4j
@Service
public class MailgunServiceImpl implements MailgunService {

    @Autowired
    private MailgunProperties mailgunProperties;

    @Autowired
    private TemplateEngine templateEngine;

    @Autowired
    private MailgunMessagesApi mailgunMessagesApi;

    @Override
    public boolean isAvailable() {
        return mailgunMessagesApi != null &&
                StrUtil.isNotBlank(mailgunProperties.getApiKey()) &&
                StrUtil.isNotBlank(mailgunProperties.getDomain());
    }

    @Override
    public void sendOrderConfirmedEmail(OrderConfirmedEvent orderConfirmedEvent) {
        if (!isAvailable()) {
            throw new IllegalStateException("Mailgun service unavailable for order confirmed; order: " + orderConfirmedEvent.getOrderId());
        }

        try {
            Context context = new Context();
            context.setVariable("orderId", orderConfirmedEvent.getOrderId());
            context.setVariable("customerId", orderConfirmedEvent.getCustomerId());
            context.setVariable("customerEmail", orderConfirmedEvent.getEmail());
            context.setVariable("region", orderConfirmedEvent.getRegion());
            context.setVariable("totalAmount", orderConfirmedEvent.getTotalAmount());
            context.setVariable("confirmedAt", orderConfirmedEvent.getConfirmedAt());
            context.setVariable("validatedBy", orderConfirmedEvent.getValidatedBy());
            context.setVariable("items", orderConfirmedEvent.getItems());
            context.setVariable("currentYear", Year.now().getValue());

            String htmlContent = templateEngine.process(EmailConstants.TEMPLATE_LOCATION + "/" + TopicConstants.ORDER_CONFIRMED, context);

            Message message = Message.builder()
                    .from(mailgunProperties.getFrom())
                    .to(orderConfirmedEvent.getEmail())
                    .subject(EmailConstants.ORDER_CONFIRMED_SUBJECT_PREFIX + orderConfirmedEvent.getOrderId())
                    .html(htmlContent)
                    .tag(TopicConstants.ORDER_CONFIRMED)
                    .tracking(true)
                    .build();

            MessageResponse messageResponse = mailgunMessagesApi.sendMessage(mailgunProperties.getDomain(), message);
            log.info("Order confirmed email sent: orderId={}, messageId={}", orderConfirmedEvent.getOrderId(), messageResponse.getId());

        } catch (Exception e) {
            log.error("Failed to send order confirmed email; order: " + orderConfirmedEvent.getOrderId(), e);
            throw new RuntimeException("Failed to send order confirmed email; order: " + orderConfirmedEvent.getOrderId(), e);
        }
    }

    @Override
    public void sendOrderFailedEmail(OrderFailedEvent orderFailedEvent) {
        if (!isAvailable()) {
            throw new IllegalStateException("Mailgun service unavailable for order failed; order: " + orderFailedEvent.getOrderId());
        }

        try {
            Context context = new Context();
            context.setVariable("orderId", orderFailedEvent.getOrderId());
            context.setVariable("customerId", orderFailedEvent.getCustomerId());
            context.setVariable("customerEmail", orderFailedEvent.getEmail());
            context.setVariable("region", orderFailedEvent.getRegion());
            context.setVariable("status", orderFailedEvent.getStatus());
            context.setVariable("failedAt", orderFailedEvent.getFailedAt());
            context.setVariable("failureReasons", orderFailedEvent.getFailureReasons());
            context.setVariable("currentYear", Year.now().getValue());

            String htmlContent = templateEngine.process(EmailConstants.TEMPLATE_LOCATION + "/" + TopicConstants.ORDER_FAILED, context);

            Message message = Message.builder()
                    .from(mailgunProperties.getFrom())
                    .to(orderFailedEvent.getEmail())
                    .subject(EmailConstants.ORDER_FAILED_SUBJECT_PREFIX + orderFailedEvent.getOrderId())
                    .html(htmlContent)
                    .tag(TopicConstants.ORDER_FAILED)
                    .build();

            MessageResponse messageResponse = mailgunMessagesApi.sendMessage(mailgunProperties.getDomain(), message);
            log.info("Order failed email sent: orderId={}, messageId={}", orderFailedEvent.getOrderId(), messageResponse.getId());

        } catch (Exception e) {
            log.error("Failed to send order failed email; order: " + orderFailedEvent.getOrderId(), e);
            throw new RuntimeException("Failed to send order failed email; order: " + orderFailedEvent.getOrderId(), e);
        }
    }
}