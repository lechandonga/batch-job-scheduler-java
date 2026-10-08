package com.github.lechandonga.batchjobschedulerjava.api;

import com.github.lechandonga.batchjobschedulerjava.api.dto.ErrorResponse;
import com.github.lechandonga.batchjobschedulerjava.core.exception.IllegalJobTransitionException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.JobNotFoundException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.RejectReason;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(JobRejectedException.class)
    public ResponseEntity<ErrorResponse> rejected(JobRejectedException e) {
        HttpStatus status = e.getReason() == RejectReason.QUEUE_FULL
                || e.getReason() == RejectReason.TENANT_QUOTA_EXCEEDED
                || e.getReason() == RejectReason.HANDLER_NOT_FOUND
                ? HttpStatus.CONFLICT : HttpStatus.BAD_REQUEST;
        return ResponseEntity.status(status).body(new ErrorResponse(e.getReason(), e.getMessage()));
    }

    @ExceptionHandler(IllegalJobTransitionException.class)
    public ResponseEntity<ErrorResponse> illegalTransition(IllegalJobTransitionException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse(e.getReason(), e.getMessage()));
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> notFound(JobNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse(RejectReason.JOB_NOT_FOUND, e.getMessage()));
    }
}
