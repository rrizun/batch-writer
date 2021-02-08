package helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

public class ConcatenatedJsonWriter {

    public interface Transport {
        /**
         * maximum transmission unit
         */
        int mtu();
        /**
         * e.g., io.micrometer tags
         */
        String[] tags();
        /**
         * send message
         */
        ListenableFuture<Void> send(String message);
    }

    class VoidFuture extends AbstractFuture<Void> {
        public boolean set(Void value) {
            return super.set(value);
        }
        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }
    }

    private final Transport transport;

    // preserve insertion order
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

        requestMeter = Metrics.counter("ConcatenatedJsonWriter.request", transport.tags());
        successMeter = Metrics.counter("ConcatenatedJsonWriter.success", transport.tags());
        failureMeter = Metrics.counter("ConcatenatedJsonWriter.failure", transport.tags());
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

        Multimap<JsonElement, VoidFuture> copyOfMessages = ImmutableMultimap.copyOf(messages);
        
        messages.clear();

        return new FutureRunner() {
            List<Runnable> defer = new ArrayList<>();
            {
                run(() -> {
                    // STEP 1 partition
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    Multimap<ByteArrayOutputStream, VoidFuture> partitions = LinkedListMultimap.create();
                    for (Entry<JsonElement, VoidFuture> entry : copyOfMessages.entries()) {
                        String jsonValue = entry.getKey().toString();
                        if (baos.size() + baos(jsonValue).size() > transport.mtu())
                            baos = new ByteArrayOutputStream();
                        new PrintStream(baos, true).println(jsonValue);
                        partitions.put(baos, entry.getValue());
                    }

                    // STEP 2 send partitions
                    for (Entry<ByteArrayOutputStream, Collection<VoidFuture>> entry : partitions.asMap().entrySet()) {
                        run(() -> {
                            return transport.send(entry.getKey().toString());
                        }, sendResponse -> {
                            for (VoidFuture voidFuture : entry.getValue()) {
                                successMeter.increment();
                                defer.add(() -> {
                                    voidFuture.set(Defaults.defaultValue(Void.class));
                                });
                            }
                        }, e -> {
                            // log(e.getMessage());
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

                    return Futures.successfulAsList(copyOfMessages.values());
                });
            }
            ByteArrayOutputStream baos(String s) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                new PrintStream(baos, true).println(s);
                return baos;
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
        String topicArn = "arn:aws:sns:us-east-1:343892718819:MyServiceDev-Myservice-TopicBFC7AF6E-QFKBW7OHVXNZ";
        final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(new ConcatenatedJsonWriterTransportAws(SnsAsyncClient.create(), topicArn));
        try {
            for (int i = 0; i < 25; ++i) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("value", random());
                writer.request(jsonObject);
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