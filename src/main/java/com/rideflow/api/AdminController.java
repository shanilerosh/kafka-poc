package com.rideflow.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.rideflow.admin.KafkaInspector;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final KafkaInspector inspector;

    public AdminController(KafkaInspector inspector) {
        this.inspector = inspector;
    }

    /** Topics with partition count, replication factor, configs, and per-partition leader / replicas / ISR. */
    @GetMapping("/topics")
    public List<Map<String, Object>> topics() throws ExecutionException, InterruptedException {
        return inspector.topics();
    }

    /** Consumer groups with state, members, committed offsets and LAG (how far behind the consumers are). */
    @GetMapping("/groups")
    public List<Map<String, Object>> groups() throws ExecutionException, InterruptedException {
        return inspector.groups();
    }
}
