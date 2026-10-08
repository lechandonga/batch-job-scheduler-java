package com.github.lechandonga.batchjobschedulerjava.api;

import com.github.lechandonga.batchjobschedulerjava.exception.JobRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把业务拒绝原因映射为可区分的 HTTP 响应：响应体 reason 为稳定的机器可读原因码，
 * 调用方可以明确区分参数非法、队列已满、租户配额不足、作业不存在、非法状态迁移等情况。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(JobRejectedException.class)
    public ResponseEntity<Map<String, Object>> handle(JobRejectedException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("reason", ex.getReason().code());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(HttpStatus.valueOf(ex.getReason().httpStatus())).body(body);
    }
}
