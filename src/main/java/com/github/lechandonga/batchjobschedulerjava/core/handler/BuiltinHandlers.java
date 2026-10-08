package com.github.lechandonga.batchjobschedulerjava.core.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Built-in demo handlers used for manual verification and tests. */
public final class BuiltinHandlers {

    private BuiltinHandlers() {
    }

    @Component
    public static class EchoHandler implements JobHandler {
        private static final Logger log = LoggerFactory.getLogger(EchoHandler.class);

        @Override
        public String jobType() {
            return "echo";
        }

        @Override
        public String execute(String payload) {
            log.info("[handler:echo] payload={}", payload);
            return "echo:" + payload;
        }
    }

    /** Payload is the sleep duration in millis; useful for timeout/cancel verification. */
    @Component
    public static class SleepHandler implements JobHandler {
        private static final Logger log = LoggerFactory.getLogger(SleepHandler.class);

        @Override
        public String jobType() {
            return "sleep";
        }

        @Override
        public String execute(String payload) throws InterruptedException {
            long millis = Long.parseLong(payload.trim());
            log.info("[handler:sleep] sleeping {} ms", millis);
            Thread.sleep(millis);
            return "slept:" + millis;
        }
    }

    /** Always fails — exercises the retry policy and the FAILED terminal state. */
    @Component
    public static class FlakyHandler implements JobHandler {
        private static final Logger log = LoggerFactory.getLogger(FlakyHandler.class);

        @Override
        public String jobType() {
            return "flaky";
        }

        @Override
        public String execute(String payload) {
            log.info("[handler:flaky] failing on purpose, payload={}", payload);
            throw new IllegalStateException("flaky handler always fails: " + payload);
        }
    }
}
