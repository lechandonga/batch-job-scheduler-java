package com.github.lechandonga.batchjobschedulerjava.api.dto;

import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;

public class SubmitJobRequest {

    private String idempotencyKey;
    private String tenantId;
    private String jobType;
    private JobPriority priority = JobPriority.NORMAL;
    private Long timeoutMillis;
    private Integer maxAttempts;
    private String payload;

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    public String getJobType() { return jobType; }
    public void setJobType(String jobType) { this.jobType = jobType; }
    public JobPriority getPriority() { return priority; }
    public void setPriority(JobPriority priority) { this.priority = priority; }
    public Long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(Long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
    public Integer getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
