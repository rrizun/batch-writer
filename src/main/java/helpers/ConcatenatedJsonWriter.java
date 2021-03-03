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

class RecordRunner<R> {
    public boolean success;
    public String failureMessage;
    public final R record;
    public static <R> RecordRunner<R> record(R record) {
        return new RecordRunner<R>(record);
    }
    private RecordRunner(R record) {
        this.record = record;
    }
    public RecordRunner<R> metric(String metricName) {
        return this;
    }
    public <V> ListenableFuture<V> run(AsyncFunction<R, V> async) {
        try {
            ListenableFuture<V> lf = async.apply(record);
            lf.addListener(() -> {
                try {
                    lf.get();
                    success = true;
                    Metrics.counter(record.getClass().getName(), "success", "true").increment();
                } catch (Exception e) {
                    failureMessage = ""+e;
                    Metrics.counter(record.getClass().getName(), "success", "false").increment();
                } finally {
                    log(SplunkHelper.render(this));
                }
            }, MoreExecutors.directExecutor());
            return lf;
        } catch (Exception e) {
            failureMessage = ""+e;
            Metrics.counter(record.getClass().getName(), "success", "false").increment();
            try {
                return Futures.immediateFailedFuture(e);
            } finally {
                log(SplunkHelper.render(this));
            }
        }
    }
    private void log(Object... args) {
        new LogHelper(record).log(args);
    }
}

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

        //###TODO UGLY
        //###TODO UGLY
        //###TODO UGLY
        in = registry.counter(WriteRecord.class.getName(), "success", "true");
        //###TODO UGLY
        //###TODO UGLY
        //###TODO UGLY
        inError = registry.counter(WriteRecord.class.getName(), "success", "false");
        //###TODO UGLY
        //###TODO UGLY
        //###TODO UGLY
        out = registry.counter("ConcatenatedJsonWriter.out", tags);
        outError = registry.counter("ConcatenatedJsonWriter.outError", tags);
    }


    private class WriteRecord {
        public String firstThree;
    }

    /**
     * write
     * 
     * @param jsonElement
     * @return
     */
    public ListenableFuture<?> write(JsonElement jsonElement) {
        return RecordRunner.record(new WriteRecord()).run(record->{
            if (jsonElement.toString().length()>3)
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
        });
    }

    private class FlushRecord {
        long in;
        long inErr;
        long out;
        long outErr;
    }

    /**
     * flush
     * 
     * @return
     */
    public ListenableFuture<?> flush() {
        return RecordRunner.record(new FlushRecord()).run(record->{
            if (baos.size() > 0)
                baos = flush(baos, partitions.get(baos));
            return Futures.whenAllComplete(partitions.values()).run(()->{
                record.in = count(in);
                record.inErr = count(inError);
                record.out = count(out);
                record.outErr = count(outError);
                // return Futures.immediateVoidFuture();    
            }, MoreExecutors.directExecutor());
            // return Futures.successfulAsList(partitions.values());
        });
    }

    // returns new baos
    private ByteArrayOutputStream flush(ByteArrayOutputStream baos, Iterable<VoidFuture> partition) {
        new FutureFacade() { // front facade not interesting.. inside futures interesting
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
                    outError.increment();
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