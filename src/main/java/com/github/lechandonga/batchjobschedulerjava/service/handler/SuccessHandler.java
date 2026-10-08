package com.github.lechandonga.batchjobschedulerjava.service.handler;

import org.springframework.stereotype.Component;

/** 恒成功处理器，payload 原样作为结果返回。 */
@Component
public class SuccessHandler implements JobHandler {

    public static final String TYPE = "success";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String execute(String payload, int attempt) {
        return "ok:" + payload;
    }
}
