package com.github.lechandonga.batchjobschedulerjava.config;

import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandlerRegistry;
import com.github.lechandonga.batchjobschedulerjava.core.quota.TenantQuotaManager;
import com.github.lechandonga.batchjobschedulerjava.core.scheduler.JobScheduler;
import com.github.lechandonga.batchjobschedulerjava.core.store.JobStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfig {

    @Bean
    public JobStore jobStore() {
        return new JobStore();
    }

    @Bean
    public TenantQuotaManager tenantQuotaManager() {
        return new TenantQuotaManager();
    }

    @Bean
    public JobHandlerRegistry jobHandlerRegistry(java.util.List<com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler> handlers) {
        return new JobHandlerRegistry(handlers);
    }

    @Bean
    public JobScheduler jobScheduler(JobStore store, TenantQuotaManager quotaManager,
                                     JobHandlerRegistry registry, SchedulerProperties props) {
        return new JobScheduler(store, quotaManager, registry, props);
    }
}
