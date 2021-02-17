package helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import io.micrometer.core.instrument.MeterRegistry;
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

    private class VoidFuture extends AbstractFuture<Void> {
        public boolean set(Void value) {
            return super.set(value);
        }
        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }
    }

    private final Transport transport;

    private final Counter requestMeter;
    // private final Counter successMeter;
    // private final Counter failureMeter;

    // preserve insertion order
    private final Multimap<JsonElement, VoidFuture> messages = LinkedListMultimap.create();

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
        // successMeter = Metrics.counter("ConcatenatedJsonWriter.success", transport.tags());
        // failureMeter = Metrics.counter("ConcatenatedJsonWriter.failure", transport.tags());
    }

    public ListenableFuture<Void> write(JsonElement message) {
        // log("write", message);
        requestMeter.increment();
        VoidFuture f = new VoidFuture();
        messages.put(message, f);
        return f;
    }

    public ListenableFuture<Void> flush() {
        log("flush");
        Multimap<JsonElement, VoidFuture> copyOfMessages = ImmutableMultimap.copyOf(messages);
        messages.clear();
        return flush(copyOfMessages, transport);
    }

    private static ListenableFuture<Void> flush(Multimap<JsonElement, VoidFuture> copyOfMessages, Transport transport) {
        Counter requestMeter = Metrics.counter("ConcatenatedJsonWriter.request", transport.tags());
        Counter successMeter = Metrics.counter("ConcatenatedJsonWriter.success", transport.tags());
        Counter failureMeter = Metrics.counter("ConcatenatedJsonWriter.failure", transport.tags());
        return new FutureRunner() {
            List<Runnable> defer = new ArrayList<>();
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
                    for (Entry<ByteArrayOutputStream, Collection<VoidFuture>> entry : partitions.asMap().entrySet()) {
                        run(() -> {
                            System.out.println(entry.getKey().toString());
                            return transport.send(entry.getKey().toString());
                        }, sendResponse -> {
                            for (VoidFuture voidFuture : entry.getValue()) {
                                successMeter.increment();
                                defer.add(() -> {
                                    voidFuture.set(Defaults.defaultValue(Void.class));
                                });
                            }
                        }, e -> {
                            for (VoidFuture voidFuture : entry.getValue()) {
                                failureMeter.increment();
                                defer.add(() -> {
                                    voidFuture.setException(e);
                                });
                            }
                        }, () -> {
                            System.out.println("stats="+stats());
                            for (Runnable runnable : defer)
                                runnable.run();
                        });
                    }

                    return Futures.successfulAsList(copyOfMessages.values());
                });
            }
            byte[] render(JsonElement jsonElement) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                new PrintStream(baos, true).println(jsonElement.toString());
                return baos.toByteArray();
            }
            String stats() {
                return new LogHelper(this).str("request", requestMeter.count(), "success", successMeter.count(), "failure", failureMeter.count());
            }
            @Override
            protected void afterDone() {
                // System.out.println("stats="+stats());
            }
        };

        //###YODO adddListener and dump stats here?
        //###YODO adddListener and dump stats here?
        //###YODO adddListener and dump stats here?

    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

    public static void main(String... args) throws Exception {
        Metrics.addRegistry(new SimpleMeterRegistry());
        String topicArn = "arn:aws:sns:us-east-1:343892718819:MyServiceDev-Myservice-TopicBFC7AF6E-QFKBW7OHVXNZ";
        final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(new ConcatenatedJsonWriterTransportAwsTopic(SnsAsyncClient.create(), topicArn));
        try {
            for (int i = 0; i < 16*250; ++i) {
                JsonObject jsonElement = new JsonObject();
                jsonElement.addProperty("value", random());
                ListenableFuture<Void> lf = writer.write(jsonElement);
                lf.addListener(()->{
                    try {
                        lf.get();
                    } catch (Exception e) {
                        System.out.println(""+e);
                    }
                }, MoreExecutors.directExecutor());
            }
        } finally {
            writer.flush().get();
        }
        System.out.println("done");
    }

    // e.g., X2OJkPuG1z5EjZlSWum54wJK
    private static String random() {
        byte[] bytes = new byte[new Random().nextInt(256)];
        new SecureRandom().nextBytes(bytes);
        return BaseEncoding.base64Url().encode(bytes);
    }

}