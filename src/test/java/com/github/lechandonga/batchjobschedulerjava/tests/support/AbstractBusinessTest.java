package com.github.lechandonga.batchjobschedulerjava.tests.support;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * 所有业务测试的基类，统一挂载 {@link BusinessTestLogger}，
 * 使每个测试类/用例在控制台输出业务化的中文日志。
 */
@ExtendWith(BusinessTestLogger.class)
public abstract class AbstractBusinessTest {
}
