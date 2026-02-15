package io.clementleetimfu.orderenotification.config;

import com.mailgun.api.v3.MailgunMessagesApi;
import com.mailgun.client.MailgunClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MailgunProperties.class)
public class MailgunConfig {
    @Bean
    public MailgunMessagesApi mailgunMessagesApi(MailgunProperties mailgunProperties) {
        return MailgunClient.config(mailgunProperties.getApiKey())
                .createAsyncApi(MailgunMessagesApi.class);
    }
}