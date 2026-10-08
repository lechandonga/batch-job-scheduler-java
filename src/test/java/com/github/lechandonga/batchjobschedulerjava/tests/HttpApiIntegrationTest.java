package com.github.lechandonga.batchjobschedulerjava.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("业务4: HTTP 接口 - 提交/查询/取消与拒绝原因的线上可区分性")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(HttpApiIntegrationTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @SuppressWarnings("rawtypes")
    private Map submit(Map<String, Object> body) {
        return rest.postForObject(url("/api/jobs"), body, Map.class);
    }

    @Test
    @DisplayName("[CASE:HTTP端到端] 提交echo作业→查询到SUCCEEDED；重复幂等键返回同一jobId")
    void submitQueryAndIdempotentHttp() throws Exception {
        log.info("[CASE:HTTP端到端] start: POST /api/jobs (echo), 轮询 GET, 再重复提交");
        Map<String, Object> body = Map.of(
                "idempotencyKey", "http-1",
                "tenantId", "tenant-web",
                "jobType", "echo",
                "payload", "hello");
        Map first = submit(body);
        String jobId = (String) first.get("jobId");
        assertEquals("QUEUED", first.get("status"));

        Map second = submit(body);
        assertEquals(jobId, second.get("jobId"), "重复提交必须返回同一个作业");
        assertEquals(Boolean.TRUE, second.get("reused"));

        String status;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        do {
            Map view = rest.getForObject(url("/api/jobs/" + jobId), Map.class);
            status = (String) view.get("status");
            if ("SUCCEEDED".equals(status)) {
                assertEquals("echo:hello", view.get("result"));
                break;
            }
            Thread.sleep(20);
        } while (System.nanoTime() < deadline);
        assertEquals("SUCCEEDED", status);
        log.info("[CASE:HTTP端到端] pass: jobId={} SUCCEEDED, result=echo:hello, 重复提交命中同一作业", jobId);
    }

    @Test
    @DisplayName("[CASE:HTTP拒绝原因] 非法参数→400 INVALID_PARAMETER；未知作业不存在→404 JOB_NOT_FOUND")
    void httpErrorReasonsAreDistinguishable() {
        log.info("[CASE:HTTP拒绝原因] start: 空 tenantId 与未知 jobId");
        ResponseEntity<Map> bad = rest.postForEntity(url("/api/jobs"),
                Map.of("idempotencyKey", "x", "tenantId", "", "jobType", "echo"), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, bad.getStatusCode());
        assertEquals("INVALID_PARAMETER", ((Map<?, ?>) bad.getBody()).get("reason"));

        ResponseEntity<Map> missing = rest.getForEntity(url("/api/jobs/no-such-id"), Map.class);
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
        assertEquals("JOB_NOT_FOUND", ((Map<?, ?>) missing.getBody()).get("reason"));
        log.info("[CASE:HTTP拒绝原因] pass: 400/INVALID_PARAMETER 与 404/JOB_NOT_FOUND 可区分");
    }

    @Test
    @DisplayName("[CASE:HTTP取消] 终态作业取消返回 409 ILLEGAL_STATE_TRANSITION")
    @SuppressWarnings("rawtypes")
    void cancelTerminalJobReturns409() throws Exception {
        log.info("[CASE:HTTP取消] start: 作业完成后 POST cancel");
        Map body = Map.of("idempotencyKey", "http-cancel-1", "tenantId", "tenant-web",
                "jobType", "echo", "payload", "x");
        Map created = submit(body);
        String jobId = (String) created.get("jobId");
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (!"SUCCEEDED".equals(rest.getForObject(url("/api/jobs/" + jobId), Map.class).get("status"))
                && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        ResponseEntity<Map> resp = rest.exchange(url("/api/jobs/" + jobId + "/cancel"),
                HttpMethod.POST, HttpEntity.EMPTY, Map.class);
        assertEquals(HttpStatus.CONFLICT, resp.getStatusCode());
        assertEquals("ILLEGAL_STATE_TRANSITION", resp.getBody().get("reason"));
        assertTrue(resp.getBody().get("message").toString().contains("SUCCEEDED"));
        log.info("[CASE:HTTP取消] pass: 409 ILLEGAL_STATE_TRANSITION, message 携带当前状态");
    }
}
