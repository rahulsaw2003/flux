package consumer;
import com.google.protobuf.ByteString;
import proto.Assignment;
import proto.ProtocolMetadata;

import java.nio.charset.StandardCharsets;
import java.util.*;

// TODO: RESEARCH ENCODING/DECODING perhaps use Kyro again? (Everything Below)
public final class ProtocolCodec {

    public static ProtocolMetadata buildProtocolMetadata(Collection<String> topics, String assignor) {
        byte[] md = encodeMetadata(topics, "assignor=" + assignor);
        return ProtocolMetadata.newBuilder()
                .setName(assignor)
                .setMetadata(ByteString.copyFrom(md))
                .build();
    }

    public static Assignment packAssignment(Map<String, List<Integer>> topicPartitions) {
        byte[] bytes = encodeAssignment(topicPartitions);
        return Assignment.newBuilder()
                .setAssignment(ByteString.copyFrom(bytes))
                .build();
    }

    public static Map<String, List<Integer>> unpackAssignment(Assignment a) {
        return decodeAssignment(a.getAssignment().toByteArray());
    }

    static byte[] encodeMetadata(Collection<String> topics, String... pairs) {
        return "".getBytes((StandardCharsets.UTF_8));
    }
    static byte[] encodeAssignment(Map<String, List<Integer>> topicPartitions) {
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            return "".getBytes(StandardCharsets.UTF_8);
        }

        StringBuilder sb = new StringBuilder();
        boolean firstTopic = true;
        for (Map.Entry<String, List<Integer>> entry : topicPartitions.entrySet()) {
            if (!firstTopic) sb.append(';');
            firstTopic = false;

            String topic = entry.getKey() == null ? "" : entry.getKey();
            sb.append(topic).append('=');

            List<Integer> partitions = entry.getValue() == null ? Collections.emptyList() : entry.getValue();
            for (int i = 0; i < partitions.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(partitions.get(i));
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    static byte[] encodeFullGroupAssignment(Map<String, Map<String, List<Integer>>> full) {
        StringBuilder sb = new StringBuilder();
        boolean firstMember = true;

        for (Map.Entry<String, Map<String, List<Integer>>> me : full.entrySet()) {
            if (!firstMember) sb.append("||");
            firstMember = false;

            String memberId = me.getKey();
            sb.append(memberId == null ? "" : memberId).append(':');

            Map<String, List<Integer>> tp = me.getValue();
            if (tp == null || tp.isEmpty()) continue;

            boolean firstTopic = true;
            for (Map.Entry<String, List<Integer>> te : tp.entrySet()) {
                if (!firstTopic) sb.append(';');
                firstTopic = false;

                String topic = te.getKey() == null ? "" : te.getKey();
                sb.append(topic).append('=');

                List<Integer> parts = te.getValue() == null ? Collections.emptyList()
                        : new ArrayList<>(te.getValue());
                Collections.sort(parts);
                for (int i = 0; i < parts.size(); i++) {
                    if (i > 0) sb.append(',');
                    sb.append(parts.get(i));
                }
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    static Map<String, List<Integer>> decodeAssignment(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new LinkedHashMap<>();
        }

        String encoded = new String(bytes, StandardCharsets.UTF_8);
        if (encoded.isEmpty()) {
            return new LinkedHashMap<>();
        }

        Map<String, List<Integer>> result = new LinkedHashMap<>();
        String[] topicEntries = encoded.split(";");

        for (String topicEntry : topicEntries) {
            if (topicEntry.isEmpty()) continue;

            String[] parts = topicEntry.split("=", 2);
            if (parts.length != 2) continue;

            String topic = parts[0];
            String partitionsStr = parts[1];

            List<Integer> partitions = new ArrayList<>();
            if (!partitionsStr.isEmpty()) {
                String[] partitionIds = partitionsStr.split(",");
                for (String pid : partitionIds) {
                    try {
                        partitions.add(Integer.parseInt(pid.trim()));
                    } catch (NumberFormatException e) {
                        // Skip invalid partition IDs
                    }
                }
            }

            result.put(topic, partitions);
        }

        return result;
    }

    /**
     * Decode full group assignment and extract a specific member's assignment
     * Format: "member1:topic1=0,1,2;topic2=3,4||member2:topic1=5,6||..."
     */
    public static Map<String, List<Integer>> extractMemberAssignment(byte[] fullAssignmentBytes, String memberId) {
        if (fullAssignmentBytes == null || fullAssignmentBytes.length == 0 || memberId == null) {
            return new LinkedHashMap<>();
        }

        String fullEncoded = new String(fullAssignmentBytes, StandardCharsets.UTF_8);
        if (fullEncoded.isEmpty()) {
            return new LinkedHashMap<>();
        }

        // Split by member: "member1:...||member2:..."
        String[] memberEntries = fullEncoded.split("\\|\\|");

        for (String memberEntry : memberEntries) {
            if (memberEntry.isEmpty()) continue;

            // Split member ID from assignment: "memberX:topic1=0,1;topic2=2,3"
            String[] parts = memberEntry.split(":", 2);
            if (parts.length != 2) continue;

            String currentMemberId = parts[0];
            String assignmentPart = parts[1];

            if (memberId.equals(currentMemberId)) {
                // Found the target member, decode their assignment
                return decodeAssignment(assignmentPart.getBytes(StandardCharsets.UTF_8));
            }
        }

        // Member not found in assignment
        return new LinkedHashMap<>();
    }
}
