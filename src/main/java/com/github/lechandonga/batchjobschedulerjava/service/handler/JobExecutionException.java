package com.github.lechandonga.batchjobschedulerjava.service.handler;

/**
 * 处理器抛出的业务异常。retryable=false 表示不可重试，作业立即 FAILED（NON_RETRYABLE）。
 */
public class JobExecutionException extends Exception {

    private final boolean retryable;

    public JobExecutionException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
