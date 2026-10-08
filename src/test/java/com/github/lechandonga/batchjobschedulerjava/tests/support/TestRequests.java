package com.github.lechandonga.batchjobschedulerjava.tests.support;

import com.github.lechandonga.batchjobschedulerjava.model.SubmitJobRequest;

public final class TestRequests {

    private TestRequests() {
    }

    public static SubmitJobRequest of(String key, String tenant, int priority, String handler,
                                      String payload, Long timeoutMillis, Integer maxAttempts) {
        SubmitJobRequest request = new SubmitJobRequest();
        request.setIdempotencyKey(key);
        request.setTenantId(tenant);
        request.setPriority(priority);
        request.setHandlerType(handler);
        request.setPayload(payload);
        request.setTimeoutMillis(timeoutMillis);
        request.setMaxAttempts(maxAttempts);
        return request;
    }

    public static SubmitJobRequest of(String key, String tenant, int priority, String handler) {
        return of(key, tenant, priority, handler, null, null, null);
    }
}
