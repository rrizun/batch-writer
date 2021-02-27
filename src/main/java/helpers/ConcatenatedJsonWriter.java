package helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.Random;

import com.google.common.base.Defaults;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

/**
 * ConcatenatedJsonWriter
 * 
 * <p>pipelined
 * <p>not thread-safe
 */
public class ConcatenatedJsonWriter {

    public interface Transport {
        /**
         * maximum transmission unit
         */
        int mtu();
        /**
         * send message
         * 
         * <p>ConcatenatedJsonWriter shall not ask Transport to send a message greater than mtu
         */
        ListenableFuture<?> send(String message);
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

    public final Counter in;
    public final Counter inError;
    public final Counter out;
    public final Counter outError;

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final Multimap<ByteArrayOutputStream, VoidFuture> partitions = Multimaps.synchronizedMultimap(LinkedListMultimap.create());

    /**
     * ctor
     * 
     * @param transport
     * @param tags
     */
    public ConcatenatedJsonWriter(Transport transport, MeterRegistry registry, String[] tags) {
        log("ctor");
        
        this.transport = transport;

        in = registry.counter("ConcatenatedJsonWriter.in", tags);
        inError = registry.counter("ConcatenatedJsonWriter.inError", tags);
        out = registry.counter("ConcatenatedJsonWriter.out", tags);
        outError = registry.counter("ConcatenatedJsonWriter.outError", tags);
    }

    /**
     * write
     * 
     * @param jsonElement
     * @return
     */
    public ListenableFuture<?> write(JsonElement jsonElement) {
        // log("write", jsonElement);
        VoidFuture lf = new VoidFuture();
        try {
            byte[] bytes = render(jsonElement);
            if (bytes.length > transport.mtu())
                throw new IllegalArgumentException(jsonElement.toString());
            if (baos.size() + bytes.length > transport.mtu())
                baos = flush(baos, partitions.get(baos));
            baos.write(bytes, 0, bytes.length);
            partitions.put(baos, lf); // track futures on a per-baos/partition basis
        } catch (Exception e) {
            if (lf.setException(e))
                inError.increment();;
        }
        return lf;
    }

    /**
     * flush
     * 
     * @return
     */
    public ListenableFuture<?> flush() {
        log("flush");
        if (baos.size() > 0)
            baos = flush(baos, partitions.get(baos));
        return Futures.successfulAsList(partitions.values());
    }

    // returns new baos
    private ByteArrayOutputStream flush(ByteArrayOutputStream baos, Iterable<VoidFuture> partition) {
        new FutureRunner2() {
            {
                run(() -> {
                    // request
                    return transport.send(baos.toString());
                }, sendResponse -> {
                    // success
                    out.increment();
                    partition.forEach(lf -> {
                        if (lf.setVoid())
                            in.increment();
                    });
                }, e -> {
                    // failure
                    outError.increment();
                    partition.forEach(lf -> {
                        if (lf.setException(e))
                            inError.increment();
                    });
                }, () -> {
                    // finally
                    log(String.format("in %s/%s out %s/%s", count(in), count(inError), count(out), count(outError)));
                });
            }
        };
        return new ByteArrayOutputStream();
    }

    private long count(Counter counter) {
        return Double.valueOf(counter.count()).longValue();
    }

    private byte[] render(JsonElement jsonElement) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new PrintStream(baos, true).println(jsonElement.toString());
        return baos.toByteArray();
    }

    private void log(Object... args) {
        new LogHelper(ConcatenatedJsonWriter.class).log(args);
    }

    public static void main(String... args) throws Exception {
        String topicArn = "arn:aws:sns:us-east-1:343892718819:MyServiceDev-Myservice-TopicBFC7AF6E-QFKBW7OHVXNZ";
        ConcatenatedJsonWriterTransportAwsTopic transport = new ConcatenatedJsonWriterTransportAwsTopic(SnsAsyncClient.create(), topicArn);
        String[] tags = new String[]{"topicArn", topicArn};
        final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport, new SimpleMeterRegistry(), tags);
        try {
            for (int i = 0; i < 16*250; ++i) {
                JsonObject jsonObject = new JsonObject();
                byte[] bytes = new byte[new Random().nextInt(256)];
                new SecureRandom().nextBytes(bytes);
                jsonObject.addProperty("value", BaseEncoding.base64Url().encode(bytes));
                ListenableFuture<?> lf = writer.write(jsonObject);
                lf.addListener(()->{
                    try {
                        lf.get();
                    } catch (Exception e) {
                        // log(e);
                    }
                }, MoreExecutors.directExecutor());
            }
        } finally {
            writer.flush().get();
        }
    }

}