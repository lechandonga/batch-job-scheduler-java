package com.github.lechandonga.batchjobschedulerjava.tests.support;

import com.github.lechandonga.batchjobschedulerjava.service.handler.JobHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 测试用可控处理器：前 blockCount 次执行在栅栏处等待（用于占满工作线程制造排队），
 * 放行后所有执行快速通过；按完成顺序记录 payload，供优先级/公平性断言。
 */
public class LatchHandler implements JobHandler {

    private final String type;
    private final int blockCount;
    private final CountDownLatch releaseGate = new CountDownLatch(1);
    private final List<String> completionOrder = java.util.Collections.synchronizedList(new ArrayList<>());

    public LatchHandler(String type, int blockCount) {
        this.type = type;
        this.blockCount = blockCount;
    }

    public void release() {
        releaseGate.countDown();
    }

    public List<String> order() {
        synchronized (completionOrder) {
            return new ArrayList<>(completionOrder);
        }
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public String execute(String payload, int attempt) throws InterruptedException {
        if (completionOrder.size() < blockCount) {
            if (!releaseGate.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("LatchHandler 等待放行超时");
            }
        }
        completionOrder.add(payload);
        return "latch:" + payload;
    }
}
