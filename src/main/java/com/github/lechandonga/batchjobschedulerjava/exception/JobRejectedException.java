package com.github.lechandonga.batchjobschedulerjava.exception;

/**
 * 提交被拒绝。不同拒绝原因通过 {@link RejectReason} 明确区分；
 * 抛出该异常意味着提交未生效，不占用队列容量或租户配额。
 */
public class JobRejectedException extends RuntimeException {

    private final RejectReason reason;

    public JobRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
