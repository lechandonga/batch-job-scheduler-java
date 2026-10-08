package com.github.lechandonga.batchjobschedulerjava.tests.support;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * 测试日志扩展：在单测日志中清晰打印“业务模块 -> 用例 -> 结论(通过/失败/中止/跳过)”，
 * 让 mvn test 的控制台输出可以直接看出覆盖了哪些业务以及每个用例的执行情况。
 */
public class BusinessTestLogger implements BeforeAllCallback, BeforeTestExecutionCallback, TestWatcher {

    @Override
    public void beforeAll(ExtensionContext context) {
        Logger log = LoggerFactory.getLogger(context.getRequiredTestClass());
        log.info("");
        log.info("==============================================================");
        log.info("业务模块开始: {}", context.getDisplayName());
        log.info("==============================================================");
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        LoggerFactory.getLogger(context.getRequiredTestClass()).info(
                "------> 开始用例 [{}]", context.getDisplayName());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        LoggerFactory.getLogger(context.getRequiredTestClass()).info(
                "<====== 用例通过 [{}]", context.getDisplayName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        LoggerFactory.getLogger(context.getRequiredTestClass()).error(
                "<XXXXXX 用例失败 [{}] 原因: {}", context.getDisplayName(),
                cause == null ? "" : cause.toString());
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        LoggerFactory.getLogger(context.getRequiredTestClass()).warn(
                "<!!!!!! 用例中止 [{}]", context.getDisplayName());
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional reason) {
        LoggerFactory.getLogger(context.getRequiredTestClass()).warn(
                "<------ 用例跳过 [{}] 原因: {}", context.getDisplayName(), reason.orElse(""));
    }
}
