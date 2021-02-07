package helpers;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class ConcatenatedJsonTopicPublisher {

    private final String topicArn;
    private final SnsAsyncClient snsClient = SnsAsyncClient.create();
    
    // preserves insertion order
    private final Multimap<JsonElement, VoidFuture> messages = LinkedListMultimap.create();

    // private final LocalMeter requestMeter = new LocalMeter();
    // private final LocalMeter successMeter = new LocalMeter();
    // private final LocalMeter failureMeter = new LocalMeter();

    private final Counter requestMeter;
    private final Counter successMeter;
    private final Counter failureMeter;

    /**
     * ctor
     * 
     * @param topicArn
     * @throws Exception
     */
    public ConcatenatedJsonTopicPublisher(String topicArn) throws Exception {
        log("ctor", topicArn);
        this.topicArn = topicArn;

        requestMeter = Metrics.counter("ConcatenatedJsonTopicPublisher.request", "topicArn", topicArn);
        successMeter = Metrics.counter("ConcatenatedJsonTopicPublisher.success", "topicArn", topicArn);
        failureMeter = Metrics.counter("ConcatenatedJsonTopicPublisher.failure", "topicArn", topicArn);

        log(failureMeter.getClass());
    }

    public ListenableFuture<Void> request(JsonElement message) {
        // log("request", message);
        requestMeter.increment();
        VoidFuture vf = new VoidFuture();
        messages.put(message, vf);
        return vf;
    }

    public ListenableFuture<Void> publish() throws Exception {
        log("flush");
        return new FutureRunner() {
            List<Runnable> defer = new ArrayList<>();
            {
                run(() -> {
                    // STEP 1 partition
                    StringBuilder sb = new StringBuilder();
                    Multimap<StringBuilder, VoidFuture> partitions = LinkedListMultimap.create();
                    for (Entry<JsonElement, VoidFuture> entry : messages.entries()) {
                        String jsonValue = entry.getKey().toString();
                        if (sb.length() + jsonValue.length() > 256*1024) // sns/sqs max msg len
                            sb = new StringBuilder();
                        sb.append(jsonValue);
                        partitions.put(sb, entry.getValue());
                    }

                    // STEP 2 publish partitions
                    for (Entry<StringBuilder, Collection<VoidFuture>> entry : partitions.asMap().entrySet()) {
                        run(() -> {
                            String message = entry.getKey().toString();
                            PublishRequest publishRequest = PublishRequest.builder()
                                    //
                                    .topicArn(topicArn).message(message).build();
                            return lf(snsClient.publish(publishRequest));
                        }, publishResponse -> {
                            for (VoidFuture voidFuture : entry.getValue()) {
                                successMeter.increment();
                                defer.add(()->{
                                    voidFuture.setVoid();
                                });
                            }
                        }, e -> {
                            log(e.getMessage());
                            for (VoidFuture voidFuture : entry.getValue()) {
                                failureMeter.increment();
                                defer.add(()->{
                                    voidFuture.setException(e);
                                });
                            }
                        }, () -> {
                            log("stats", stats());
                            for (Runnable runnable: defer)
                                runnable.run();
                        });
                    }

                    return Futures.successfulAsList(messages.values());
                });
            }
        };
    }

    private String stats() {
        return new LogHelper(this).str("request", requestMeter.count(), "success", successMeter.count(), "failure", failureMeter.count());
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

    public static void main(String... args) throws Exception {
        Metrics.addRegistry(new SimpleMeterRegistry());
        final ConcatenatedJsonTopicPublisher topicPublisher = new ConcatenatedJsonTopicPublisher("arn:aws:sns:us-east-1:343892718819:MyServiceDev-Myservice-TopicBFC7AF6E-QFKBW7OHVXNZ");
        try {
            for (int i = 0; i < 250000; ++i) {
                topicPublisher.request(new JsonPrimitive(random()));
            }
        } finally {
            topicPublisher.publish().get();
        }
        System.out.println("done");
    }

    // e.g., X2OJkPuG1z5EjZlSWum54wJK
    private static String random() {
        byte[] bytes = new byte[18];
        new SecureRandom().nextBytes(bytes);
        return BaseEncoding.base64Url().encode(bytes);
    }

}