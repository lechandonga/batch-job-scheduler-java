package com.github.lechandonga.batchjobschedulerjava.core.model;

/**
 * Job lifecycle states.
 *
 * Legal transitions (see {@link #canTransitTo(JobStatus)}):
 * <pre>
 * QUEUED  -> RUNNING | CANCELLED
 * RUNNING -> SUCCEEDED | FAILED | TIMED_OUT | QUEUED(retry backoff) | CANCELLED
 * SUCCEEDED / FAILED / CANCELLED / TIMED_OUT are terminal.
 * </pre>
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    TIMED_OUT;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED || this == TIMED_OUT;
    }

    public boolean canTransitTo(JobStatus target) {
        return switch (this) {
            case QUEUED -> target == RUNNING || target == CANCELLED;
            // RUNNING -> QUEUED means the job is parked for retry backoff (quota already released).
            case RUNNING -> target == SUCCEEDED || target == FAILED || target == TIMED_OUT
                    || target == CANCELLED || target == QUEUED;
            default -> false;
        };
    }
}
