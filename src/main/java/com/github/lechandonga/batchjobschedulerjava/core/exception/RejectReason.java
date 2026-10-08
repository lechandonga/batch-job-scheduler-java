package com.github.lechandonga.batchjobschedulerjava.core.exception;

/** Distinguishable rejection/failure reasons so callers never get one ambiguous error. */
public enum RejectReason {
    INVALID_PARAMETER,
    IDEMPOTENT_CONFLICT,
    QUEUE_FULL,
    TENANT_QUOTA_EXCEEDED,
    JOB_NOT_FOUND,
    ILLEGAL_STATE_TRANSITION,
    HANDLER_NOT_FOUND
}
