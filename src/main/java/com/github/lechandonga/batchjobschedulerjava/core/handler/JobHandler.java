package com.github.lechandonga.batchjobschedulerjava.core.handler;

/**
 * Business logic executed for one job attempt. Implementations must be thread-safe;
 * a single instance may be invoked concurrently for different jobs.
 */
public interface JobHandler {

    String jobType();

    /**
     * @param payload submission payload
     * @return the business result stored on the job
     * @throws Exception any failure triggers the retry policy; the scheduler never propagates it
     */
    String execute(String payload) throws Exception;
}
