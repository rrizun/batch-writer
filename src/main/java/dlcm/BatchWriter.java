package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import com.spotify.futures.CompletableFuturesExtra;

import helpers.LogHelper;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class BatchWriter {

    // config
    private final boolean compress; // true to use gzip compression
    private final long lingerMs;

    // batch thread
    private final ScheduledExecutorService batchThread = Executors.newSingleThreadScheduledExecutor();

    // batch thread state
    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    private int userRecordBatchCount;
    private ScheduledFuture<?> scheduledPublishFuture;

    // private static final int DEFAULT_MAX_CONNECTIONS = 50;
    // private static final int DEFAULT_MAX_CONNECTION_ACQUIRES = 10_000;

    private final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
            //
            // .maxConcurrency(250)
    //
    ;

    private final SnsAsyncClient sns = SnsAsyncClient.builder()
    //
    // .httpClientBuilder(httpClientBuilder)
    
    //
    .build();

    private final String topicArn = "arn:aws:sns:us-east-2:743203956339:DlcmStack-InputEventTopicC39C99C1-QBIUZXL0AN";

    private int requestCount;
    private int confirmCount;
    private int errorCount;

    private final MyMeter requestRate = new MyMeter(5);
    private final MyMeter confirmRate = new MyMeter(5);
    private final MyMeter errorRate = new MyMeter(5);

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
        batchThread.execute(() -> {
            try {
                jsonWriter.endArray();
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        synchronized(busyCond) {
            while (busy>0)
                busyCond.wait();
        }
        MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * addUserRecord
     */
    public void addUserRecord(JsonElement jsonElement) {
        ++requestCount;
        requestRate.mark(1);

        batchThread.execute(() -> {
            try {
                String jsonValue = jsonElement.toString();

                // will this exceed the max msg len?
                final int MAX_MSG_LEN = 256 * 1024; // sns/sqs 256KB

                // the -1 is for closing the json array with a ']'
                if (baos.size() + jsonValue.length() > MAX_MSG_LEN - 1) {

                    // yes- publish now
                    jsonWriter.endArray();
                    publishNow(); // does not block
                    jsonWriter.beginArray();

                }

                jsonWriter.jsonValue(jsonValue);
                jsonWriter.flush();

                ++userRecordBatchCount;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void schedulePublish() {
        scheduledPublishFuture = batchThread.schedule(() -> {
            try {
                jsonWriter.endArray();
                publishNow(); // does not block
                jsonWriter.beginArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, lingerMs, TimeUnit.MILLISECONDS);
    }

    private void cancelScheduledPublish() {
        scheduledPublishFuture.cancel(false);
    }

    // this is run within the batchPool context
    private void publishNow() {

        stats();

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

            final int finalUserRecordBatchCount = userRecordBatchCount; // take snapshot

            ListenableFuture<PublishResponse> listenableFuture = lf(sns.publish(publishRequest));
            ++busy;
            stats();
            listenableFuture.addListener(()->{
                try {
                    PublishResponse publishResponse = listenableFuture.get();
                    trace(publishResponse);

                    confirmCount += finalUserRecordBatchCount;
                    confirmRate.mark(finalUserRecordBatchCount);
                } catch (Exception e) {
                    log(e);
                    errorCount += finalUserRecordBatchCount;
                    errorRate.mark(finalUserRecordBatchCount);
                } finally {
                    synchronized(busyCond) {
                        --busy;
                        busyCond.notifyAll();
                    }
                    stats();
                }
            }, batchThread);

            // STEP 3 start new batch
            baos.reset();
            jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
            userRecordBatchCount = 0;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // reschedule scheduled publish
            schedulePublish();
        }

    }

    public static void main(String... args) throws Exception {

        // en0:        1693.58 KB/s         2863.68 KB/s         4557.27 KB/s
        
        // start time
        final long t0 = System.currentTimeMillis();
        
        final BatchWriter topicWriter = new BatchWriter(false, 2000);
        try {
            System.out.println("start");
            topicWriter.start();

            int rate = 7500;
            if (args.length>0)
                rate=Integer.parseInt(args[0]);
            System.out.println("rate:"+rate);
            final RateLimiter rateLimiter = RateLimiter.create(rate); // per second
            for (int i = 0; i < 30*rate ; ++i) {
                JsonObject userRecord = new JsonObject();
                String key = Hashing.sha256().hashInt(i%rate).toString();
                userRecord.addProperty("entityKey", key);
                userRecord.addProperty("entityType", "/foo/bar/baz");
                userRecord.addProperty("version", System.currentTimeMillis());

                // does not block
                 topicWriter.addUserRecord(userRecord);

                // rate limit
                rateLimiter.acquire();
            }

        } finally {
            System.out.println("close");
            topicWriter.close();
            System.out.println("closed");
        }
        
        // finish time
        System.out.println((System.currentTimeMillis() - t0)+"ms");

    }

    private void stats() {
        log(
            String.format("request=%s/%s", requestRate.average(), requestCount),
            String.format("success=%s/%s", confirmRate.average(), confirmCount),
            String.format("failure=%s/%s", errorRate.average(), errorCount),
            // "errorCount", errorCount
            ""
            );
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