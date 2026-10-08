package com.github.lechandonga.batchjobschedulerjava.service.handler;

import org.springframework.stereotype.Component;

/**
 * 可重试失败处理器：前 failTimes 次尝试都以“可重试失败”结束，之后成功。
 * payload 为 failTimes（缺省 1）。
 */
@Component
public class FailHandler implements JobHandler {

    public static final String TYPE = "fail";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String execute(String payload, int attempt) throws JobExecutionException {
        int failTimes = 1;
        if (payload != null && !payload.isBlank()) {
            failTimes = Integer.parseInt(payload.trim());
        }
        if (attempt <= failTimes) {
            throw new JobExecutionException("模拟第 " + attempt + " 次失败", true);
        }
        return "recovered-after-" + (attempt - 1) + "-retries";
    }
}
