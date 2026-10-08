package com.github.lechandonga.batchjobschedulerjava.model;

/**
 * 作业进入终态的最终结论，查询结果始终有且仅有一个结论，不会自相矛盾。
 */
public enum TerminalReason {
    SUCCESS,
    FAILURE,
    RETRY_EXHAUSTED,
    CANCELED_WHILE_QUEUED,
    CANCELED_WHILE_RUNNING,
    ATTEMPT_TIMED_OUT,
    RETRY_EXHAUSTED_TIMEOUT,
    NON_RETRYABLE
}
