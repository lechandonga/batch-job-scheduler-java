package com.github.lechandonga.batchjobschedulerjava.service.handler;

import org.springframework.stereotype.Component;

/**
 * 睡眠处理器：执行 payload 毫秒（缺省 1000ms），响应中断，用于测试超时与取消运行中作业。
 */
@Component
public class SleepHandler implements JobHandler {

    public static final String TYPE = "sleep";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String execute(String payload, int attempt) throws InterruptedException {
        long sleepMillis = 1_000L;
        if (payload != null && !payload.isBlank()) {
            sleepMillis = Long.parseLong(payload.trim());
        }
        Thread.sleep(sleepMillis);
        return "slept-" + sleepMillis + "ms";
    }
}
