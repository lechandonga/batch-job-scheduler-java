package com.github.lechandonga.batchjobschedulerjava.core.handler;

import com.github.lechandonga.batchjobschedulerjava.core.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.RejectReason;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public JobHandlerRegistry(java.util.List<JobHandler> springHandlers) {
        springHandlers.forEach(this::register);
    }

    public void register(JobHandler handler) {
        handlers.put(handler.jobType(), handler);
    }

    public Optional<JobHandler> find(String jobType) {
        return Optional.ofNullable(handlers.get(jobType));
    }

    public JobHandler require(String jobType) {
        return find(jobType).orElseThrow(() -> new JobRejectedException(
                RejectReason.HANDLER_NOT_FOUND, "no handler registered for jobType=" + jobType));
    }
}
