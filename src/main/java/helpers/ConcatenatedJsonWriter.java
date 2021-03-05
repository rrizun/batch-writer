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
import com.google.common.util.concurrent.AsyncFunction;
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
         * <p>
         * ConcatenatedJsonWriter shall not ask Transport to send a message more than mtu
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
    public final Counter inErr;
    public final Counter out;
    public final Counter outErr;

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final Multimap<ByteArrayOutputStream, VoidFuture> partitions = Multimaps.synchronizedMultimap(LinkedListMultimap.create());

    /**
     * ctor
     * 
     * @param transport
     * @param tags
     */
    public ConcatenatedJsonWriter(Transport transport, MeterRegistry registry) {
        log("ctor");

        this.transport = transport;

        in = registry.counter("ConcatenatedJsonWriter.in", "success", "true");
        inErr = registry.counter("ConcatenatedJsonWriter.inErr", "success", "false");
        out = registry.counter("ConcatenatedJsonWriter.out", "success", "true");
        outErr = registry.counter("ConcatenatedJsonWriter.outErr", "success", "false");
    }

    class WriteRecord {
        public boolean success;
        public String failureMessage;
        public String firstThree;
        public String toString() {
            return SplunkHelper.toString(this);
        }
    }

    /**
     * write
     * 
     * @param jsonElement
     * @return
     */
    public ListenableFuture<?> write(JsonElement jsonElement) {
        return new FutureRunner() {
            WriteRecord record = new WriteRecord();
            {
                run(() -> {
                    // for fun
                    if (jsonElement.toString().length() > 3)
                        record.firstThree = jsonElement.toString().substring(0, 3);

                    byte[] bytes = render(jsonElement);
                    if (bytes.length > transport.mtu())
                        throw new IllegalArgumentException("jsonElement more than mtu");
                    if (baos.size() + bytes.length > transport.mtu())
                        baos = flush(baos, partitions.get(baos));
                    baos.write(bytes, 0, bytes.length);

                    VoidFuture lf = new VoidFuture();
                    partitions.put(baos, lf); // track futures on a per-baos/partition basis
                    return lf;
                }, result->{
                    in.increment();
                    record.success = true;
                }, e->{
                    inErr.increment();
                    record.failureMessage = e.toString();
                }, ()->{
                    log(record);
                });
            }
        }.one();
    }

    class FlushRecord {
        long in;
        long inErr;
        long out;
        long outErr;
        public String toString() {
            return SplunkHelper.toString(this);
        }
    }

    /**
     * flush
     * 
     * @return
     */
    public ListenableFuture<?> flush() {
        return new FutureRunner() {
            FlushRecord record = new FlushRecord();
            {
                run(() -> {
                    if (baos.size() > 0)
                        baos = flush(baos, partitions.get(baos));
                    return Futures.successfulAsList(partitions.values());
                }, () -> {
                    record.in = count(in);
                    record.inErr = count(inErr);
                    record.out = count(out);
                    record.outErr = count(outErr);
                    log(record);
                });
            }
        }.one();
    }

    // returns new baos
    private ByteArrayOutputStream flush(ByteArrayOutputStream baos, Iterable<VoidFuture> partition) {
        //###TODO really a fire-forget? e.g., what if want to cancel??
        //###TODO really a fire-forget? e.g., what if want to cancel??
        //###TODO really a fire-forget? e.g., what if want to cancel??
        new FutureRunner() { // front facade not interesting.. inside futures interesting
            {
                run(() -> {
                    // request
                    return transport.send(baos.toString());
                }, sendResponse -> {
                    // success
                    out.increment();
                    partition.forEach(lf -> lf.setVoid());
                }, e -> {
                    // failure
                    outErr.increment();
                    partition.forEach(lf -> lf.setException(e));
                }, () -> {
                    // finally
                    // log(String.format("in %s/%s out %s/%s", count(in), count(inError), count(out), count(outError)));
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
        final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport, new SimpleMeterRegistry());
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