package com.github.lechandonga.batchjobschedulerjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfiguration {
}
