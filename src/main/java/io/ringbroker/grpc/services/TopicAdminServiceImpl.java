package io.ringbroker.grpc.services;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.grpc.stub.StreamObserver;
import io.ringbroker.api.TopicAdminApi;
import io.ringbroker.api.TopicAdminServiceGrpc;
import io.ringbroker.registry.TopicRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
@RequiredArgsConstructor
public class TopicAdminServiceImpl extends TopicAdminServiceGrpc.TopicAdminServiceImplBase {

    private final TopicRegistry topicRegistry;

    @Override
    public void createTopic(final TopicAdminApi.CreateTopicRequest request, final StreamObserver<TopicAdminApi.TopicReply> responseObserver) {
        final String topic = request.getTopic();

        if (topicRegistry.contains(topic)) {
            responseObserver.onNext(TopicAdminApi.TopicReply.newBuilder()
                    .setSuccess(true)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        try {
            Descriptor descriptor = null;

            if (request.hasSchema()) {
                final DescriptorProto proto = request.getSchema();
                final FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                        .addMessageType(proto)
                        .setName("schema_" + topic + ".proto")
                        .setPackage("dynamic")
                        .build();

                final FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{});
                descriptor = fileDescriptor.findMessageTypeByName(proto.getName());
            }

            topicRegistry.addTopic(topic, descriptor);

            responseObserver.onNext(TopicAdminApi.TopicReply.newBuilder()
                    .setSuccess(true)
                    .build());
        } catch (final Exception e) {
            log.error("Failed to register schema for topic {}: {}", topic, e.getMessage(), e);
            responseObserver.onNext(TopicAdminApi.TopicReply.newBuilder()
                    .setSuccess(false)
                    .setError("Schema error: " + e.getMessage())
                    .build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void listTopics(final TopicAdminApi.Empty request, final StreamObserver<TopicAdminApi.TopicListReply> responseObserver) {
        final Set<String> topics = topicRegistry.listTopics();
        final TopicAdminApi.TopicListReply reply = TopicAdminApi.TopicListReply.newBuilder()
                .addAllTopics(topics)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void describeTopic(final TopicAdminApi.TopicRequest request, final StreamObserver<TopicAdminApi.TopicDescriptionReply> responseObserver) {
        final String topic = request.getTopic();

        if (!topicRegistry.contains(topic)) {
            responseObserver.onNext(TopicAdminApi.TopicDescriptionReply.newBuilder()
                    .setTopic(topic)
                    .setPartitions(0)
                    .setError("Topic not found")
                    .build());
        } else {
            // Stub value for now, update when you support dynamic partition counts
            responseObserver.onNext(TopicAdminApi.TopicDescriptionReply.newBuilder()
                    .setTopic(topic)
                    .setPartitions(1)
                    .build());
        }

        responseObserver.onCompleted();
    }
}
