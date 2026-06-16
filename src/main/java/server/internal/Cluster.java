package server.internal;

import commons.MiniKafkaExecutor;
import grpc.BrokerServer;
import metadata.Metadata;
import org.tinylog.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Cluster {
    private String clusterId; // ex: CLUSTER-1
    private List<Broker> nodes;
    private Broker controllerNode;

    public Cluster(String clusterId) {
        this.clusterId = clusterId;
        this.nodes = new ArrayList<>();
    }

    // Initializes all the brokers to be in this cluster, but does not yet start them up.
    public void bootstrapCluster(List<InetSocketAddress> bootstrapServerAddrs) throws IOException {
        // for each address, create a broker @ that particular address
        for (int i = 0; i < bootstrapServerAddrs.size(); i++) {
            Broker broker = new Broker(
                    "BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()),
                    bootstrapServerAddrs.get(i).getHostName(),
                    bootstrapServerAddrs.get(i).getPort()
            );
            nodes.add(broker);
        }

        // Pick first node by default since we currently don't have a consensus algo implemented rn
        controllerNode = nodes.get(0);
        controllerNode.setIsActiveController(true);
        String controllerEndpoint = "%s:%d".formatted(bootstrapServerAddrs.get(0).getHostName(), bootstrapServerAddrs.get(0).getPort());
        controllerNode.setControllerEndpoint(controllerEndpoint);
        controllerNode.setClusterId(this.clusterId);

        if (nodes.size() > 1) {
            // All other nodes must then store the controller node's endpoint in order to make further requests,
            // i.e., broker registration, heartbeats, etc
            List<Broker> followerNodes = nodes.subList(1, nodes.size());
            for (Broker node : followerNodes) {
                node.setControllerEndpoint(controllerNode.getControllerEndpoint());
            }
        }

        controllerNode.initControllerBrokerMetadata();
    }

    // Fire up each server, ready for requests.
    public void startCluster() {
        // Must fire up Controller node first so that it's ready to accept requests immediately
        CountDownLatch latch = new CountDownLatch(1);
        MiniKafkaExecutor.getExecutorService().submit(() -> {
            BrokerServer controllerServer = new BrokerServer(controllerNode);
            try {
                Logger.info("Starting controller broker server on port " + controllerNode.getPort());
                controllerServer.start(controllerNode.getPort());
                Logger.info("Controller broker server started successfully");
                latch.countDown();
            } catch (IOException e) {
                Logger.error("Failed to start controller broker server", e);
                throw new RuntimeException("Controller server startup failed", e);
            } catch (Exception e) {
                Logger.error("Unexpected error starting controller", e);
                throw new RuntimeException("Controller startup failed with unexpected error", e);
            }
        });

        try {
            // The thread will block until our latch gets counted down to 0 (which only happens after the controller starts up).
            // By doing this, we can ensure the controller is ready before sending any requests.
            Logger.info("Waiting for controller to start (10 second timeout)...");
            if (latch.await(10, TimeUnit.SECONDS)) {
                Logger.info("Controller started, now starting follower brokers...");
                for (Broker b : nodes) {
                    if (!b.isActiveController()) {
                        // Start up each broker in its own thread
                        MiniKafkaExecutor.getExecutorService().submit(() -> {
                            BrokerServer server = new BrokerServer(b);
                            try {
                                Logger.info("Starting follower broker " + b.getBrokerId() + " on port " + b.getPort());
                                server.start(b.getPort());
                                Logger.info("Follower broker " + b.getBrokerId() + " started successfully");
                                // Once each server is started, it will immediately make a BrokerRegistrationRequest to the controller
                                b.registerBroker(); // initial metadata state is handled here too
                            } catch (IOException e) {
                                Logger.error("Failed to start follower broker " + b.getBrokerId(), e);
                                throw new RuntimeException("Follower broker startup failed", e);
                            }
                        });
                    }
                }
            } else {
                Logger.error("Controller failed to start within 10 seconds - latch timeout!");
                throw new RuntimeException("Controller startup timeout");
            }
        } catch (InterruptedException e) {
            Logger.error("Interrupted while waiting for controller startup", e);
            throw new RuntimeException("Controller startup interrupted", e);
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<Broker> getNodes() {
        return nodes;
    }

    public Broker getControllerNode() {
        return controllerNode;
    }
}
