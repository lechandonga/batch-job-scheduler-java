package com.github.lechandonga.batchjobschedulerjava.api;

import com.github.lechandonga.batchjobschedulerjava.api.dto.JobView;
import com.github.lechandonga.batchjobschedulerjava.api.dto.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobView> submit(@RequestBody SubmitJobRequest request) {
        JobView view = jobService.submit(request);
        return ResponseEntity.status(view.reused() ? HttpStatus.OK : HttpStatus.CREATED).body(view);
    }

    @GetMapping("/{jobId}")
    public JobView get(@PathVariable String jobId) {
        return jobService.get(jobId);
    }

    @PostMapping("/{jobId}/cancel")
    public JobView cancel(@PathVariable String jobId) {
        return jobService.cancel(jobId);
    }
}
