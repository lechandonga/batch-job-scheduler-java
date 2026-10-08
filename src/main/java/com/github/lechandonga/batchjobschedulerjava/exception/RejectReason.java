package com.github.lechandonga.batchjobschedulerjava.exception;

/**
 * 提交被拒绝 / 操作失败的可区分原因码。调用方依据 {@link #code()} 区分不同拒绝原因，
 * 服务端依据 {@link #httpStatus()} 映射 HTTP 状态。
 */
public enum RejectReason {

    INVALID_PARAMETER(400),
    UNKNOWN_HANDLER(400),
    QUEUE_FULL(503),
    TENANT_QUOTA_EXCEEDED(503),
    JOB_NOT_FOUND(404),
    ILLEGAL_TRANSITION(409);

    private final int httpStatus;

    RejectReason(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String code() {
        return name();
    }

    public int httpStatus() {
        return httpStatus;
    }
}
