package helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.Random;
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
         * send message
         */
        ListenableFuture<Void> send(String message);
    }

    private class VoidFuture extends AbstractFuture<Void> {
        public boolean setVoid() {
            return super.set(Defaults.defaultValue(Void.class));
        }
        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }
    }

    private final Transport transport;

    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter failureCounter;

    // preserve insertion order
    private final Multimap<JsonElement, VoidFuture> messages = LinkedListMultimap.create();

    /**
     * ctor
     * 
     * @param transport
     * @param tags
     */
    public ConcatenatedJsonWriter(Transport transport, String[] tags) {
        log("ctor");
        
        this.transport = transport;

        requestCounter = Metrics.counter("ConcatenatedJsonWriter.request", tags);
        successCounter = Metrics.counter("ConcatenatedJsonWriter.success", tags);
        failureCounter = Metrics.counter("ConcatenatedJsonWriter.failure", tags);
    }

    public ListenableFuture<Void> write(JsonElement message) {
        // log("write", message);
        requestCounter.increment();
        VoidFuture lf = new VoidFuture();
        messages.put(message, lf);
        return lf;
    }

    public ListenableFuture<Void> flush() {
        Multimap<JsonElement, VoidFuture> copyOfMessages = ImmutableMultimap.copyOf(messages);
        messages.clear();
        return new FutureRunner2() {
            {
                run(() -> {
                    Multimap<ByteArrayOutputStream, VoidFuture> partitions = LinkedListMultimap.create();

                    // STEP 1 partition
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    for (Entry<JsonElement, VoidFuture> entry : copyOfMessages.entries()) {
                        byte[] bytes = render(entry.getKey());
                        if (baos.size() + bytes.length > transport.mtu())
                            baos = new ByteArrayOutputStream();
                        baos.write(bytes, 0, bytes.length);
                        partitions.put(baos, entry.getValue());
                    }

                    // STEP 2 send partitions
                    partitions.asMap().entrySet().forEach(entry -> {
                        run(() -> {
                            // request
                            return transport.send(entry.getKey().toString());
                        }, sendResponse -> {
                            // success
                            entry.getValue().forEach(lf -> {
                                if (lf.setVoid())
                                    successCounter.increment();
                            });
                        }, e -> {
                            // failure
                            entry.getValue().forEach(lf -> {
                                if (lf.setException(e))
                                    failureCounter.increment();
                            });
                        }, () -> {
                            // finally
                            log("request", requestCounter.count(), "success", successCounter.count(), "failure", failureCounter.count());
                        });
                    });

                    return Futures.successfulAsList(copyOfMessages.values());
                });
            }
            byte[] render(JsonElement jsonElement) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                new PrintStream(baos, true).println(jsonElement.toString());
                return baos.toByteArray();
            }
        };
    }

    static void log(Object... args) {
        new LogHelper(ConcatenatedJsonWriter.class).log(args);
    }

    public static void main(String... args) throws Exception {
        Metrics.addRegistry(new SimpleMeterRegistry());
        String topicArn = "arn:aws:sns:us-east-1:343892718819:MyServiceDev-Myservice-TopicBFC7AF6E-QFKBW7OHVXNZ";
        ConcatenatedJsonWriterTransportAwsTopic transport = new ConcatenatedJsonWriterTransportAwsTopic(SnsAsyncClient.create(), topicArn);
        String[] tags = new String[]{"topicArn", topicArn};
        final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport, tags);
        try {
            for (int i = 0; i < 16*250; ++i) {
                JsonObject jsonObject = new JsonObject();
                byte[] bytes = new byte[new Random().nextInt(256)];
                new SecureRandom().nextBytes(bytes);
                jsonObject.addProperty("value", BaseEncoding.base64Url().encode(bytes));
                ListenableFuture<Void> lf = writer.write(jsonObject);
                lf.addListener(()->{
                    try {
                        lf.get();
                    } catch (Exception e) {
                        // log(e);
                    }
                }, MoreExecutors.directExecutor());
            }
        } finally {
            log(writer.flush().get());
        }
    }

}