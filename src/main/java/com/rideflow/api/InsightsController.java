package com.rideflow.api;

import java.util.Collection;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.rideflow.consumer.DriverRegistry;
import com.rideflow.consumer.SurgeCache;
import com.rideflow.domain.Events.DriverStatusChanged;
import com.rideflow.domain.Events.SurgeUpdate;
import com.rideflow.streams.ZoneDemandQuery;

@RestController
@RequestMapping("/api")
public class InsightsController {

    private final DriverRegistry registry;
    private final SurgeCache surgeCache;
    private final ZoneDemandQuery zoneDemand;

    public InsightsController(DriverRegistry registry, SurgeCache surgeCache, ZoneDemandQuery zoneDemand) {
        this.registry = registry;
        this.surgeCache = surgeCache;
        this.zoneDemand = zoneDemand;
    }

    /** Built from the compacted driver.status topic. */
    @GetMapping("/drivers")
    public Collection<DriverStatusChanged> drivers() {
        return registry.all();
    }

    /** Output of the Kafka Streams windowed aggregation, read back from the surge.pricing topic. */
    @GetMapping("/surge")
    public Map<String, SurgeUpdate> surge() {
        return surgeCache.all();
    }

    /** Interactive query straight into the Kafka Streams state store. */
    @GetMapping("/zones/demand")
    public Map<String, Long> demand() {
        try {
            return zoneDemand.totalsPerZone();
        } catch (IllegalStateException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        }
    }
}
