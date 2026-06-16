package grpc.services;

import metadata.InMemoryTopicMetadataRepository;
import commons.IntRange;
import metadata.snapshots.BrokerMetadata;
import metadata.snapshots.PartitionMetadata;
import proto.*;
import server.internal.Broker;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;

// This service is found on top of the Controller node
public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase {
    private Broker broker;

    public MetadataServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void fetchBrokerMetadata(FetchBrokerMetadataRequest request, StreamObserver<FetchBrokerMetadataResponse> responseObserver) {
        FetchBrokerMetadataResponse response = FetchBrokerMetadataResponse
                .newBuilder()
                .setBrokerId(broker.getBrokerId())
                .setHost(broker.getHost())
                .setPortNumber(broker.getPort())
                .setNumPartitions(broker.getNumPartitions())
                .build();

        responseObserver.onNext(response); // this just sends the response back to the client
        responseObserver.onCompleted(); // lets the client know there are no more messages after this
    }

    @Override
    public void fetchClusterMetadata(FetchClusterMetadataRequest request, StreamObserver<FetchClusterMetadataResponse> responseObserver) {
        if (!broker.isActiveController()) {
            return;
        }

        FetchClusterMetadataResponse.Builder response = FetchClusterMetadataResponse.newBuilder();

        // Build ControllerDetails
        proto.ControllerDetails controllerDetails = proto.ControllerDetails
                .newBuilder()
                .setControllerId(broker.getBrokerId())
                .addAllFollowerNodeEndpoints(broker.getFollowerNodeEndpoints().values())
                .setIsActive(true)
                .build();
        response.setControllerDetails(controllerDetails);

        // We must also build out the Controller's broker metadata as well
        response.putBrokerDetails(
                "%s:%d".formatted(broker.getHost(), broker.getPort()),
                proto.BrokerDetails
                        .newBuilder()
                        .setBrokerId(this.broker.getBrokerId())
                        .setHost(this.broker.getHost())
                        .setPort(this.broker.getPort())
                        .setNumPartitions(this.broker.getNumPartitions())
                        .putAllPartitionDetails(PartitionMetadata
                                .toDetailsMapProto(this.broker
                                        .getControllerMetadata()
                                        .get()
                                        .partitionMetadata()
                                )
                        )
                        .build()
        );

        // For each broker in the cluster, create a BrokerDetails object for it and put it in the map w/ its broker addr as key
        for (Map.Entry<String, BrokerMetadata> entry : broker.getCachedFollowerMetadata().entrySet()) {
            proto.BrokerDetails details = proto.BrokerDetails
                    .newBuilder()
                    .setBrokerId(entry.getValue().brokerId())
                    .setHost(entry.getValue().host())
                    .setPort(entry.getValue().port())
                    .setNumPartitions(entry.getValue().numPartitions())
                    .putAllPartitionDetails(PartitionMetadata.toDetailsMapProto(entry.getValue().partitionMetadata()))
                    .build();

            response.putBrokerDetails(entry.getKey(), details);
        }

        // Build TopicDetails from InMemoryTopicMetadataRepository
        Map<String, proto.TopicDetails> topicDetailsMap = new HashMap<>();
        for (String topicName : InMemoryTopicMetadataRepository.getInstance().getActiveTopics()) {
            try {
                IntRange range = InMemoryTopicMetadataRepository.getInstance().getPartitionIdRangeForTopic(topicName);
                int numPartitions = range.end() - range.start() + 1;

                // Build partition details map for this topic
                Map<Integer, proto.PartitionDetails> partitionDetailsForTopic = new HashMap<>();
                for (int partitionId = range.start(); partitionId <= range.end(); partitionId++) {
                    // Find which broker hosts this partition
                    String brokerForPartition = broker.getHost() + ":" + broker.getPort(); // For now, all partitions are on controller

                    proto.PartitionDetails partitionDetails = proto.PartitionDetails.newBuilder()
                        .setPartitionId(partitionId)
                        .setBrokerId(brokerForPartition)
                        .build();

                    partitionDetailsForTopic.put(partitionId, partitionDetails);
                }

                proto.TopicDetails.Builder topicBuilder = proto.TopicDetails.newBuilder()
                    .setTopicName(topicName)
                    .setNumPartitions(numPartitions)
                    .putAllPartitionDetails(partitionDetailsForTopic);

                topicDetailsMap.put(topicName, topicBuilder.build());
            } catch (IllegalArgumentException e) {
                // Skip topics that can't be found
            }
        }
        response.putAllTopicDetails(topicDetailsMap);

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

}
