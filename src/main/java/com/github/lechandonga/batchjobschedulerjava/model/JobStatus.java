package com.github.lechandonga.batchjobschedulerjava.model;

/**
 * 作业生命周期状态。
 *
 * <p>合法迁移路径（唯一入口见
 * {@link Job#transitionTo(JobStatus, TerminalReason)}）：
 * <ul>
 *     <li>QUEUED  -> RUNNING / CANCELED</li>
 *     <li>RUNNING -> SUCCEEDED / FAILED / TIMED_OUT / CANCELED</li>
 * </ul>
 * SUCCEEDED / FAILED / CANCELED / TIMED_OUT 为终态，终态不可再迁移，
 * 任何非法迁移都会抛出 {@link com.github.lechandonga.batchjobschedulerjava.exception.IllegalJobTransitionException}。
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELED,
    TIMED_OUT;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELED || this == TIMED_OUT;
    }
}
