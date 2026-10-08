package com.github.lechandonga.batchjobschedulerjava.service.handler;

import org.springframework.stereotype.Component;

/**
 * 抛出未受检异常的处理器，用于验证处理器自身的 bug（任意 RuntimeException）
 * 会被按可重试失败隔离处理，不会击穿调度器或影响其他作业。
 */
@Component
public class BuggyHandler implements JobHandler {

    public static final String TYPE = "buggy";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String execute(String payload, int attempt) {
        throw new IllegalStateException("处理器内部未预期错误: " + payload);
    }
}
