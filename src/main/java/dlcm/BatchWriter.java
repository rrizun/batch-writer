package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.databind.deser.NullValueProvider;
import com.google.common.base.Defaults;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import com.spotify.futures.CompletableFuturesExtra;

import helpers.LogHelper;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

class VoidFuture extends AbstractFuture<Void> {
    public boolean set() {
        return super.set(Defaults.defaultValue(Void.class));
    }
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }
}

public class BatchWriter {

    // config
    private final boolean compress; // true to use gzip compression
    private final long lingerMs;

    // batch thread
    private final ExecutorService batchThread = Executors.newSingleThreadExecutor();

    // batch thread state
    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    // private int userRecordBatchCount; // number of 'app level' messages written to jsonWriter so far
    private final List<VoidFuture> userRecordFutures = new ArrayList<>();
    // private ScheduledFuture<?> scheduledPublishFuture;

    // private static final int DEFAULT_MAX_CONNECTIONS = 50;
    // private static final int DEFAULT_MAX_CONNECTION_ACQUIRES = 10_000;

    private final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
    //
    // .maxConcurrency(250)
    //
    ;

    private final SnsAsyncClient snsClient = SnsAsyncClient.builder()
            //
            // .httpClientBuilder(httpClientBuilder)
            //
            .build();

    private final String topicArn = "arn:aws:sns:us-east-2:743203956339:DlcmStack-InputEventTopicC39C99C1-QBIUZXL0AN";

    private final long periodSeconds = 5;
    private final MyMeter requestMeter = new MyMeter();
    private final MyMeter successMeter = new MyMeter();
    private final MyMeter failureMeter = new MyMeter();

    /**
     * ctor
     * 
     * @param compress
     * @param lingerMs
     * @throws Exception
     */
    public BatchWriter(boolean compress, long lingerMs) throws Exception {
        log("ctor");
        this.compress = compress;
        this.lingerMs = lingerMs;
    }

    public void start() throws Exception {
        log("start");
        batchThread.execute(() -> {
            try {
                jsonWriter.beginArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        schedulePublish();
    }

    private int busy;
    private final Object busyCond = new Object();

    public void close() throws Exception {
        log("close");
        batchThread.execute(() -> {
            try {
                jsonWriter.endArray();
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        synchronized (busyCond) {
            while (busy > 0)
                busyCond.wait();
        }
        if (MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE)))
            snsClient.close();
    }

    public ListenableFuture<Void> flush() throws Exception {
        log("flush");
        return new AbstractFuture<Void>() {
            {
                batchThread.execute(() -> {
                    try {
                        jsonWriter.endArray();
                        publishNow().addListener(()->{
                            set(Defaults.defaultValue(Void.class));
                        }, batchThread);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        };
    }

    /**
     * addUserRecord
     */
    public ListenableFuture<Void> addUserRecord(JsonElement jsonElement) {
        return new VoidFuture(){
            {
                requestMeter.mark(1);
                batchThread.execute(() -> {
                    try {
                        String jsonValue = jsonElement.toString();
        
                        final int MAX_MSG_LEN = 256 * 1024; // sns/sqs 256KB
        
                        // will this exceed the max msg len?
                        // the -1 is for closing the json array with a ']'
                        if (baos.size() + jsonValue.length() > MAX_MSG_LEN - 1) {
        
                            // yes- publish now
                            jsonWriter.endArray();
                            publishNow(); // does not block
                            jsonWriter.beginArray();
        
                        }
        
                        jsonWriter.jsonValue(jsonValue);
                        jsonWriter.flush();
        
                        userRecordFutures.add(this);
                    } catch (Exception e) {
                        setException(e);
                    }
                });
            }
        };
    }

    private void schedulePublish() {
        // scheduledPublishFuture = batchThread.schedule(() -> {
        //     try {
        //         jsonWriter.endArray();
        //         publishNow(); // does not block
        //         jsonWriter.beginArray();
        //     } catch (Exception e) {
        //         throw new RuntimeException(e);
        //     }
        // }, lingerMs, TimeUnit.MILLISECONDS);
    }

    private void cancelScheduledPublish() {
        // scheduledPublishFuture.cancel(false);
    }

    // this is run within the batchPool context
    private ListenableFuture<Void> publishNow() {
        trace("publishNow");
        return new AbstractFuture<Void>() {
            {
                // cancel scheduled publish
                cancelScheduledPublish();

                try {

                    // STEP 1 close batch
                    jsonWriter.close();

                    // STEP 2 publish
                    String[] utf8 = new String[] { new String(baos.toByteArray()) };
                    if (compress) {
                        ByteArrayOutputStream tmp = new ByteArrayOutputStream();
                        try (OutputStream out = new GZIPOutputStream(tmp)) {
                            ByteStreams.copy(new ByteArrayInputStream(baos.toByteArray()), out);
                        }
                        utf8[0] = BaseEncoding.base64().encode(tmp.toByteArray());
                    }
                    // log(baos.size(), utf8[0].length());

                    trace(utf8[0].length(), utf8[0].substring(0, Math.min(utf8[0].length(), 120)));

                    PublishRequest publishRequest = PublishRequest.builder()
                            //
                            .topicArn(topicArn)
                            //
                            .message(utf8[0])
                            //
                            .build();

                    trace(publishRequest.message().length());

                    // final int finalUserRecordBatchCount = userRecordBatchCount; // take snapshot
                    final List<VoidFuture> asdf = new ArrayList<>(userRecordFutures);

                    ListenableFuture<PublishResponse> listenableFuture = lf(snsClient.publish(publishRequest));
                    ++busy;
                    stats();
                    listenableFuture.addListener(() -> {
                        try {
                            PublishResponse publishResponse = listenableFuture.get();
                            trace(publishResponse);
                            successMeter.mark(asdf.size());
                            stats();
                            for (VoidFuture future : asdf)
                                future.set();
                        } catch (Exception e) {
                            log(e);
                            failureMeter.mark(asdf.size());
                            stats();
                            for (VoidFuture future : asdf)
                                future.setException(e);
                        } finally {
                            --busy;
                            synchronized (busyCond) {
                                busyCond.notifyAll();
                            }
                            set(Defaults.defaultValue(Void.class)); // set publishNow future result
                        }
                    }, batchThread);

                    // STEP 3 start new batch
                    baos.reset();
                    jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
                    userRecordFutures.clear();

                } catch (Exception e) {
                    setException(e); // set publishNow future exception
                } finally {
                    // reschedule scheduled publish
                    schedulePublish();
                }
            }
        };
    }

    public static void main(String... args) throws Exception {

        // en0: 1693.58 KB/s 2863.68 KB/s 4557.27 KB/s

        // start time
        final long t0 = System.currentTimeMillis();

        final BatchWriter topicWriter = new BatchWriter(false, 2000);
        try {
            System.out.println("start");
            topicWriter.start();

            int rate = 7500;
            if (args.length > 0)
                rate = Integer.parseInt(args[0]);
            System.out.println("rate:" + rate);
            final RateLimiter rateLimiter = RateLimiter.create(rate); // per second

            List<ListenableFuture<Void>> sync = new ArrayList<>();
            for (int i = 0; i < 15 * rate; ++i) {
                JsonObject userRecord = new JsonObject();
                String key = Hashing.sha256().hashInt(i % rate).toString();
                userRecord.addProperty("entityKey", key);
                userRecord.addProperty("entityType", "/foo/bar/baz");
                userRecord.addProperty("version", System.currentTimeMillis());

                // does not block
                sync.add(topicWriter.addUserRecord(userRecord));

                // rate limit
                rateLimiter.acquire();
            }

            System.out.println("call flush.get[1]");
            topicWriter.flush();
            System.out.println("call flush.get[2]");

            System.out.println("call sync.get[1]");
            Futures.allAsList(sync).get();
            System.out.println("call sync.get[2]");

        } finally {
            System.out.println("close[1]");
            topicWriter.close();
            System.out.println("close[2]");
        }

        // finish time
        System.out.println((System.currentTimeMillis() - t0) + "ms");

    }

    private void stats() {
        log("request", requestMeter, "success", successMeter, "failure", failureMeter);
    }

    private <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
        return CompletableFuturesExtra.toListenableFuture(cf);
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

    private void trace(Object... args) {
        // new LogHelper(this).log(args);
    }

}