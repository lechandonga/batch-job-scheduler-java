package com.github.lechandonga.batchjobschedulerjava.service.handler;

import org.springframework.stereotype.Component;

/** 不可重试失败处理器：立即 FAILED（NON_RETRYABLE），不消耗重试次数。 */
@Component
public class FatalHandler implements JobHandler {

    public static final String TYPE = "fatal";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String execute(String payload, int attempt) throws JobExecutionException {
        throw new JobExecutionException("不可重试错误: " + payload, false);
    }
}
