package com.rideflow.admin;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** Uses Kafka's AdminClient API to look at the cluster: topics, partitions, leaders, ISR, consumer groups, lag. */
@Component
public class KafkaInspector {

    private static final List<String> INTERESTING_CONFIGS = List.of("cleanup.policy", "retention.ms", "min.insync.replicas");

    private final KafkaAdmin kafkaAdmin;

    public KafkaInspector(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public List<Map<String, Object>> topics() throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> names = admin.listTopics().names().get().stream().filter(n -> !n.startsWith("__")).collect(Collectors.toCollection(TreeSet::new));
            var descriptions = admin.describeTopics(names).allTopicNames().get();
            var configs = admin.describeConfigs(names.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).toList()).all().get();

            List<Map<String, Object>> out = new ArrayList<>();
            for (String name : names) {
                var d = descriptions.get(name);
                var cfg = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name));
                var topic = new LinkedHashMap<String, Object>();
                topic.put("topic", name);
                topic.put("partitions", d.partitions().size());
                topic.put("replicationFactor", d.partitions().get(0).replicas().size());
                var c = new LinkedHashMap<String, String>();
                INTERESTING_CONFIGS.forEach(k -> c.put(k, cfg.get(k).value()));
                topic.put("config", c);
                topic.put("layout", d.partitions().stream().map(this::layout).toList());
                out.add(topic);
            }
            return out;
        }
    }

    private String layout(TopicPartitionInfo p) {
        return "p%d leader=broker%d replicas=%s isr=%s".formatted(p.partition(), p.leader() == null ? -1 : p.leader().id(), ids(p.replicas()), ids(p.isr()));
    }

    private List<Integer> ids(List<Node> nodes) {
        return nodes.stream().map(Node::id).sorted().toList();
    }

    public List<Map<String, Object>> groups() throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            List<String> ids = admin.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).sorted().toList();
            var described = admin.describeConsumerGroups(ids).all().get();

            List<Map<String, Object>> out = new ArrayList<>();
            for (String id : ids) {
                var d = described.get(id);
                var offsets = admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata().get();
                var ends = admin.listOffsets(offsets.keySet().stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))).all().get();

                long totalLag = 0;
                List<String> partitions = new ArrayList<>();
                for (var e : new TreeMap<>(offsets.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().toString(), x -> x))).values()) {
                    TopicPartition tp = e.getKey();
                    long lag = ends.get(tp).offset() - e.getValue().offset();
                    totalLag += lag;
                    partitions.add("%s committed=%d end=%d lag=%d".formatted(tp, e.getValue().offset(), ends.get(tp).offset(), lag));
                }
                var g = new LinkedHashMap<String, Object>();
                g.put("group", id);
                g.put("state", d.state().toString());
                g.put("members", d.members().stream().map(m -> m.clientId() + " -> " + m.assignment().topicPartitions().size() + " partitions").toList());
                g.put("totalLag", totalLag);
                g.put("partitions", partitions);
                out.add(g);
            }
            return out;
        }
    }
}
