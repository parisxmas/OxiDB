package com.oxidb.spring;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(OxiDbProperties.class)
public class OxiDbAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OxiDbClient oxidbClient(OxiDbProperties properties) {
        return new OxiDbClient(properties.getHost(), properties.getPort(), properties.getTimeoutMs());
    }
}
