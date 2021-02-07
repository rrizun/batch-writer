package helpers;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.spotify.futures.CompletableFuturesExtra;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class ConcatenatedJsonWriter {

    public interface Transport {
        int mtu();
        ListenableFuture<Void> send(String message);
    }

    private final Transport transport;
    
    // preserves insertion order
    private final Multimap<JsonElement, VoidFuture> messages = LinkedListMultimap.create();

    private final Counter requestMeter;
    private final Counter successMeter;
    private final Counter failureMeter;

    /**
     * ctor
     * 
     * @param topicArn
     * @throws Exception
     */
    public ConcatenatedJsonWriter(Transport transport) {
        log("ctor");
        this.transport = transport;

        String topicArn = "TODOFIXMETODO";

        requestMeter = Metrics.counter("ConcatenatedJsonTopicPublisher.request", "topicArn", topicArn);
        successMeter = Metrics.counter("ConcatenatedJsonTopicPublisher.success", "topicArn", topicArn);
        failureMeter = Metrics.counter("ConcatenatedJsonTopicPublisher.failure", "topicArn", topicArn);
    }

    public ListenableFuture<Void> request(JsonElement message) {
        // log("request", message);
        requestMeter.increment();
        VoidFuture vf = new VoidFuture();
        messages.put(message, vf);
        return vf;
    }

    public ListenableFuture<Void> send() throws Exception {
        log("send");

        Multimap<JsonElement, VoidFuture> copyOfMessage = ImmutableMultimap.copyOf(messages);
        messages.clear();

        return new FutureRunner() {
            List<Runnable> defer = new ArrayList<>();
            {
                run(() -> {
                    // STEP 1 partition
                    StringBuilder sb = new StringBuilder();
                    //###TODO USE STRINGWRITER TO HANDLE CR/LF
                    //###TODO USE STRINGWRITER TO HANDLE CR/LF
                    //###TODO USE STRINGWRITER TO HANDLE CR/LF
                    Multimap<StringBuilder, VoidFuture> partitions = LinkedListMultimap.create();
                    for (Entry<JsonElement, VoidFuture> entry : copyOfMessage.entries()) {
                        String jsonValue = entry.getKey().toString();
                        if (sb.length() + jsonValue.length() > transport.mtu())
                            sb = new StringBuilder();
                        sb.append(jsonValue);
                        partitions.put(sb, entry.getValue());
                    }

                    // STEP 2 send partitions
                    for (Entry<StringBuilder, Collection<VoidFuture>> entry : partitions.asMap().entrySet()) {
                        run(() -> {
                            return transport.send(entry.getKey().toString());
                        }, sendResponse -> {
                            for (VoidFuture voidFuture : entry.getValue()) {
                                successMeter.increment();
                                defer.add(() -> {
                                    voidFuture.setVoid();
                                });
                            }
                        }, e -> {
                            log(e.getMessage());
                            for (VoidFuture voidFuture : entry.getValue()) {
                                failureMeter.increment();
                                defer.add(() -> {
                                    voidFuture.setException(e);
                                });
                            }
                        }, () -> {
                            log("stats", stats());
                            for (Runnable runnable : defer)
                                runnable.run();
                        });
                    }

                    return Futures.successfulAsList(copyOfMessage.values());
                });
            }
        };
    }

    private String stats() {
        return new LogHelper(this).str("request", requestMeter.count(), "success", successMeter.count(), "failure",
                failureMeter.count());
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

    public static void main(String... args) throws Exception {

        ConcatenatedJsonWriter.Transport aws = new ConcatenatedJsonWriter.Transport() {
            SnsAsyncClient snsClient = SnsAsyncClient.create();
            String topicArn = "arn:aws:sns:us-east-1:343892718819:MyServiceDev-Myservice-TopicBFC7AF6E-QFKBW7OHVXNZ";
    
            @Override
            public int mtu() {
                return 256 * 1024; // sns/sqs max msg len
            }
    
            @Override
            public ListenableFuture<Void> send(String message) {
                return new AbstractFuture<Void>() {
                    {
                        PublishRequest publishRequest = PublishRequest.builder()
                                //
                                .topicArn(topicArn).message(message).build();
                        ListenableFuture<PublishResponse> lf = CompletableFuturesExtra.toListenableFuture(snsClient.publish(publishRequest));
                        lf.addListener(() -> {
                            try {
                                lf.get();
                                set(Defaults.defaultValue(Void.class));
                            } catch (Exception e) {
                                setException(e);
                            }
                        }, MoreExecutors.directExecutor());
                    }
                };
            }
        };
    
        final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(aws);
        try {
            for (int i = 0; i < 25000; ++i) {
                writer.request(new JsonPrimitive(random()));
            }
        } finally {
            writer.send().get();
        }
        System.out.println("done");
    }

    // e.g., X2OJkPuG1z5EjZlSWum54wJK
    private static String random() {
        byte[] bytes = new byte[18];
        new SecureRandom().nextBytes(bytes);
        return BaseEncoding.base64Url().encode(bytes);
    }

    static {
        Metrics.addRegistry(new SimpleMeterRegistry());
    }

}