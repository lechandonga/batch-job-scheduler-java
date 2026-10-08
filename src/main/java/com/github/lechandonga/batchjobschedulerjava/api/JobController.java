package com.github.lechandonga.batchjobschedulerjava.api;

import com.github.lechandonga.batchjobschedulerjava.model.JobView;
import com.github.lechandonga.batchjobschedulerjava.model.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.service.scheduler.BatchJobScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 批量作业 REST 接口：
 * <ul>
 *     <li>POST /api/jobs              提交作业（幂等）</li>
 *     <li>GET  /api/jobs/{id}         按作业ID查询</li>
 *     <li>GET  /api/jobs/idempotent/{key} 按幂等键查询</li>
 *     <li>POST /api/jobs/{id}/cancel  取消作业</li>
 *     <li>GET  /api/tenants/{id}/quota 查询租户配额占用</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class JobController {

    private final BatchJobScheduler scheduler;

    public JobController(BatchJobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostMapping("/jobs")
    public JobView submit(@RequestBody SubmitJobRequest request) {
        return scheduler.submit(request);
    }

    @GetMapping("/jobs/{jobId}")
    public JobView get(@PathVariable String jobId) {
        return scheduler.getJob(jobId);
    }

    @GetMapping("/jobs/idempotent/{idempotencyKey}")
    public JobView getByIdempotencyKey(@PathVariable String idempotencyKey) {
        return scheduler.getByIdempotencyKey(idempotencyKey);
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public JobView cancel(@PathVariable String jobId) {
        return scheduler.cancel(jobId);
    }

    @GetMapping("/tenants/{tenantId}/quota")
    public Map<String, Object> tenantQuota(@PathVariable String tenantId) {
        return scheduler.tenantQuotaView(tenantId);
    }
}
