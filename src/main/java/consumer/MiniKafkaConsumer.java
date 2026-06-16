package consumer;

import commons.MiniKafkaExecutor;
import commons.headers.Headers;
import consumer.assignors.PartitionAssignor;
import consumer.assignors.RangeAssignor;
import consumer.assignors.RoundRobinAssignor;
import consumer.assignors.StickyAssignor;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class MiniKafkaConsumer<K, V> implements Consumer {
    private final ConsumerServiceGrpc.ConsumerServiceBlockingStub blockingStub;
    private final GroupCoordinatorServiceGrpc.GroupCoordinatorServiceBlockingStub groupCoordinatorServiceBlockingStub;
    private final MetadataServiceGrpc.MetadataServiceBlockingStub metadataServiceBlockingStub;
    private ManagedChannel channel;
    private GroupCoordinator groupCoordinatorClient;

    private String groupId = "my-group";
    private String memberId = "";
    private int generationId = -1;
    private String leaderId = "";
    private boolean isLeader = false;
    private Assignment myAssignment;

    private Collection<String> subscribedTopics = List.of();
    private final Map<String, List<Integer>> assignedTopicPartitions = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Integer>> partitionOffsets = new ConcurrentHashMap<>(); // topic -> (partition -> offset)
    private volatile Assignment assignment;
    private ScheduledExecutorService hbExec;
    private ScheduledFuture<?> hbTask;

    private int sessionTimeoutsMs = 10_000;
    private int rebalanceTimoutMs = 30_000;


    public MiniKafkaConsumer() {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = ConsumerServiceGrpc.newBlockingStub(channel);
        groupCoordinatorServiceBlockingStub = GroupCoordinatorServiceGrpc.newBlockingStub(channel);
        metadataServiceBlockingStub = MetadataServiceGrpc.newBlockingStub(channel);
        groupCoordinatorClient = new GroupCoordinator(groupCoordinatorServiceBlockingStub);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        this.subscribedTopics = new ArrayList<>(topics);


        //TODO: Figure out how get from members itself.
        List<ProtocolMetadata> protocols = List.of(
                ProtocolCodec.buildProtocolMetadata(this.subscribedTopics, "range"),
                ProtocolCodec.buildProtocolMetadata(this.subscribedTopics, "roundrobin")
        );

        JoinGroupResponse joinResponse = groupCoordinatorClient.joinGroupLoop(
                groupId,
                (memberId == null ? "": memberId),
                sessionTimeoutsMs,
                rebalanceTimoutMs,
                protocols
        );

        this.memberId = joinResponse.getMemberId();
        this.generationId = joinResponse.getGenerationId();
        this.leaderId = joinResponse.getLeaderId();
        this.isLeader = this.memberId.equals(this.leaderId);

        // CASE 1: Build FULL assignment => send all followers info with SyncGroup.
        if (isLeader) {
            // Populate members.
            List<String> memberIds = new ArrayList<>();
            if (joinResponse.getMembersCount() > 0){
                for (MemberInfo info: joinResponse.getMembersList()) {
                    memberIds.add(info.getMemberId());
                }
            }

            // EDGE CASE: we missed ourselves from the total list.
            if (!memberIds.contains(memberId)) memberIds.add(memberId);

            Logger.info("Leader {} received {} members in JoinGroupResponse: {}", memberId, memberIds.size(), memberIds);

            // Choose assignor based on negotiated protocol
            PartitionAssignor assignor = selectAssignor(joinResponse.getProtocol());

            // topic -> total partitions registered
            Map<String, Integer> topicToPartitionCount = fetchPartitionCounts(subscribedTopics);

            LeaderAssignmentPlanner planner = new LeaderAssignmentPlanner(assignor);
            Assignment groupBlob = planner.buildGroupAssignment(memberIds, subscribedTopics, topicToPartitionCount);

            Logger.info("Leader {} built assignment with topicToPartitionCount: {}", memberId, topicToPartitionCount);
            Logger.info("Leader {} sending full assignment: {} bytes, content: {}",
                memberId, groupBlob.getAssignment().size(), groupBlob.getAssignment().toStringUtf8());

            // Leader sends the full assignment
            SyncGroupResponse sync = groupCoordinatorServiceBlockingStub.syncGroup( // TODO: Missing SyncGroup.
                    SyncGroupRequest.newBuilder()
                            .setGroupId(groupId)
                            .setGenerationId(generationId)
                            .setMemberId(memberId)
                            .setAssignment(groupBlob)
                            .build()
            );

            // Server returns my member-specific slice
            this.myAssignment = sync.getAssignment();
        }
        else { // CASE 2: We are a Follower, get own assignment.
            SyncGroupResponse sync = groupCoordinatorServiceBlockingStub.syncGroup( // TODO: Missing SyncGroup.
                    SyncGroupRequest.newBuilder()
                            .setGroupId(groupId)
                            .setGenerationId(generationId)
                            .setMemberId(memberId)
                            .setAssignment(Assignment.getDefaultInstance())
                            .build()
            );
            this.myAssignment = sync.getAssignment();
        }

        // TODO: Install my assignment for poll()
        Logger.info("Member {} received assignment from SyncGroup: {} bytes, content: {}",
            memberId, this.myAssignment.getAssignment().size(), this.myAssignment.getAssignment().toStringUtf8());
        Map<String, List<Integer>> tp = ProtocolCodec.unpackAssignment(this.myAssignment);
        Logger.info("Member {} unpacked assignment: {}", memberId, tp);
        installAssignment(tp);

        // TODO: Start heartbeats
        groupCoordinatorClient.startHeartBeat();
    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public PollResult poll(Duration timeout) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();

        // If no partitions assigned, return empty
        if (assignedTopicPartitions.isEmpty()) {
            Logger.warn("No partitions assigned to this consumer");
            return new PollResult(records, true);
        }

        // Fetch from all assigned partitions
        for (Map.Entry<String, List<Integer>> entry : assignedTopicPartitions.entrySet()) {
            String topic = entry.getKey();
            List<Integer> partitions = entry.getValue();

            for (Integer partitionId : partitions) {
                // Get current offset for this partition (default to 0)
                int offset = partitionOffsets
                        .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                        .getOrDefault(partitionId, 0);

                FetchMessageRequest req = FetchMessageRequest
                        .newBuilder()
                        .setTopic(topic)
                        .setPartitionId(partitionId)
                        .setStartingOffset(offset)
                        .build();

                try {
                    // push fetch message execution into worker thread
                    Future<FetchMessageResponse> future = MiniKafkaExecutor.getExecutorService()
                            .submit(() -> blockingStub.fetchMessage(req));
                    // waits for task to complete for at most the given timeout
                    FetchMessageResponse response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                    if (response.getStatus().equals(Status.READ_COMPLETION)) {
                        // No more messages in this partition, continue to next
                        continue;
                    }

                    Message msg = response.getMessage();
                    ConsumerRecord<String, String> record = new ConsumerRecord<>(
                            msg.getTopic(),
                            msg.getPartition(),
                            msg.getOffset(),
                            msg.getTimestamp(),
                            msg.getKey(),
                            msg.getValue(),
                            new Headers()
                    );
                    records.add(record);

                    // Update offset for this partition
                    partitionOffsets.get(topic).put(partitionId, msg.getOffset() + 1);

                } catch (TimeoutException e) {
                    // Timeout for this partition, continue to next
                    Logger.trace("Timeout fetching from {}:{}", topic, partitionId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new PollResult(records, false);
                } catch (ExecutionException e) {
                    Logger.error("Fetch failed for {}:{} - {}", topic, partitionId, e.getCause().getMessage());
                }
            }
        }

        return new PollResult(records, !records.isEmpty());
    }

    @Override
    public void commitOffsets() {

    }

    private PartitionAssignor selectAssignor(String protocol) {
        if (protocol == null) protocol = "range";
        return switch (protocol.toLowerCase()) {
            case "roundrobin" -> new RoundRobinAssignor();
            case "sticky" -> new StickyAssignor(); // TODO: We are still missing implementation.
            default -> new RangeAssignor();
        };
    }

    private Map<String, Integer> fetchPartitionCounts(Collection<String> topics) {
        Map<String, Integer> counts = new HashMap<>();

        // Fetch cluster metadata from broker via gRPC
        try {
            FetchClusterMetadataRequest request = FetchClusterMetadataRequest.newBuilder().build();
            FetchClusterMetadataResponse response = metadataServiceBlockingStub.fetchClusterMetadata(request);

            Logger.info("Fetched cluster metadata - available topics: {}", response.getTopicDetailsMap().keySet());

            for (String topic : topics) {
                if (topic != null && !topic.isEmpty()) {
                    proto.TopicDetails topicDetails = response.getTopicDetailsMap().get(topic);
                    if (topicDetails != null) {
                        counts.put(topic, topicDetails.getNumPartitions());
                        Logger.info("Fetched metadata for topic {}: {} partitions", topic, topicDetails.getNumPartitions());
                    } else {
                        Logger.warn("Topic {} not found in cluster metadata", topic);
                    }
                }
            }
        } catch (Exception e) {
            Logger.error("Failed to fetch cluster metadata: {}", e.getMessage());
            e.printStackTrace();
        }

        Logger.info("Final partition counts map: {}", counts);
        return counts;
    }

    private void installAssignment(Map<String, List<Integer>> tp) {
        assignedTopicPartitions.clear();
        if (tp != null) {
            for (Map.Entry<String, List<Integer>> e : tp.entrySet()) {
                // copy to avoid external mutation
                assignedTopicPartitions.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
        }
        Logger.info("Installed assignment: " + assignedTopicPartitions);
        // TODO: If fetch API needs it, build/refresh a local fetch plan from this map.
    }

}
