package com.github.lechandonga.batchjobschedulerjava.service.handler;

/**
 * 作业处理器 SPI。实现应响应线程中断（{@link Thread#interrupted()}），
 * 以便取消与超时控制能及时生效。处理器抛出的任何异常都被隔离，不会影响其他作业或调度器。
 */
public interface JobHandler {

    /** 处理器类型标识，对应提交参数 handlerType。 */
    String type();

    /**
     * @param payload 作业负载
     * @param attempt 当前尝试次数，从 1 开始
     * @return 成功时的结果消息
     */
    String execute(String payload, int attempt) throws JobExecutionException, InterruptedException;
}
