package com.oxidb.example;

import com.oxidb.client.OxiDbClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

/**
 * Spring Boot wiring for the OxiDB client. The {@code @Bean} below is
 * the manual equivalent of what a future {@code oxidb-spring-boot-starter}
 * would generate via {@code @AutoConfiguration} — kept explicit here so
 * the integration story is obvious to readers who clone this sample.
 */
@SpringBootApplication
public class ExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }

    /**
     * Single shared {@link OxiDbClient} for the whole app. The client is
     * thread-safe (internal lock serialises requests on its socket), so
     * one bean is enough. For very high concurrency, configure a small
     * pool of these — out of scope for this demo.
     */
    @Bean(destroyMethod = "close")
    public OxiDbClient oxiDbClient(
            @Value("${oxidb.host:127.0.0.1}") String host,
            @Value("${oxidb.port:4444}") int port) throws IOException {
        return OxiDbClient.connect(host, port);
    }
}
