package com.github.lechandonga.batchjobschedulerjava.service.handler;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 处理器注册表。Spring 容器中的所有 {@link JobHandler} 自动注册，
 * 测试也可通过 {@link #register(JobHandler)} 动态注入临时处理器。
 */
@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public JobHandlerRegistry(java.util.List<JobHandler> springHandlers) {
        springHandlers.forEach(this::register);
    }

    public void register(JobHandler handler) {
        handlers.put(handler.type(), handler);
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public java.util.Set<String> registeredTypes() {
        return handlers.keySet();
    }
}
