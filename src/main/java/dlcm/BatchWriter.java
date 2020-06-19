package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import helpers.LogHelper;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsClient;
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
    private ScheduledFuture<?> scheduledPublishFuture;

    // publish threads
    private final ExecutorService publishPool = Executors.newCachedThreadPool();

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

    private final MetricRegistry metricRegistry = new MetricRegistry();

    private final Meter meter = metricRegistry.meter("asdf");

    /**
     * ctor
     * 
     * @param compress
     * @param lingerMs
     * @throws Exception
     */
    public BatchWriter(boolean compress, long lingerMs) throws Exception {
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

    public void close() throws Exception {
        batchThread.execute(() -> {
            try {
                jsonWriter.endArray();
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // batchPool.shutdown();
        // batchPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE));

        // publishPool.shutdown();
        // publishPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        // MoreExecutors.shutdownAndAwaitTermination(publishPool, Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * addUserRecord
     */
    public void addUserRecord(JsonElement jsonElement) {
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
        scheduledPublishFuture.cancel(true);
    }

    // this is run within the batchPool context
    private void publishNow() throws Exception {

        // cancel scheduled publish
        cancelScheduledPublish();

        // STEP 1 close
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

        publishPool.execute(()->{

            try {
                log(utf8[0].length(), utf8[0].substring(0, Math.min(utf8[0].length(), 120)));

                PublishRequest request = PublishRequest.builder()
                        //
                        .topicArn(topicArn)
                        //
                        .message(utf8[0])
                        //
                        .build();
    
                // PublishResponse response =
                sns.publish(request).whenComplete((a, b) -> {
                    log(a, b);
                    if (b!=null)
                        b.printStackTrace();
                });
                // log(response);
    
                // //###TODO do actual sns publish here
                // try {
                // Thread.sleep(2000);
                // } catch (Exception e) {
                // throw new RuntimeException(e);
                // }
    
            } catch (Exception e) {
                e.printStackTrace();;
            }

        });

        // STEP 3 start
        baos.reset();
        jsonWriter = new JsonWriter(new OutputStreamWriter(baos));

        // reschedule scheduled publish
        schedulePublish();

    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }
 
//     Caused by: software.amazon.awssdk.core.exception.SdkClientException: Unable to execute HTTP request: Acquire operation took longer than the configured maximum time. This indicates that a request cannot get a connection from the pool within the specified maximum time. This can be due to high request rate.
// Consider taking any of the following actions to mitigate the issue: increase max connections, increase acquire timeout, or slowing the request rate.
// Increasing the max connections can increase client throughput (unless the network interface is already fully utilized), but can eventually start to hit operation system limitations on the number of file descriptors used by the process. If you already are fully utilizing your network interface or cannot further increase your connection count, increasing the acquire timeout gives extra time for requests to acquire a connection before timing out. If the connections doesn't free up, the subsequent requests will still timeout.
// If the above mechanisms are not able to fix the issue, try smoothing out your requests so that large traffic bursts cannot overload the client, being more efficient with the number of times you need to call AWS, or by increasing the number of hosts sending requests.
//         at software.amazon.awssdk.core.exception.SdkClientException$BuilderImpl.build(SdkClientException.java:98)
//         at software.amazon.awssdk.core.exception.SdkClientException.create(SdkClientException.java:43)
//         at software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper.setLastException(RetryableStageHelper.java:199)
//         at software.amazon.awssdk.core.internal.http.pipeline.stages.AsyncRetryableStage$RetryingExecutor.maybeRetryExecute(AsyncRetryableStage.java:143)
//         ... 18 more

    /**
     * Acquire operation took longer than the configured maximum time. This
     * indicates that a request cannot get a connection from the pool within the
     * specified maximum time. This can be due to high request rate. Consider taking
     * any of the following actions to mitigate the issue: increase max connections,
     * increase acquire timeout, or slowing the request rate. Increasing the max
     * connections can increase client throughput (unless the network interface is
     * already fully utilized), but can eventually start to hit operation system
     * limitations on the number of file descriptors used by the process. If you
     * already are fully utilizing your network interface or cannot further increase
     * your connection count, increasing the acquire timeout gives extra time for
     * requests to acquire a connection before timing out. If the connections
     * doesn't free up, the subsequent requests will still timeout. If the above
     * mechanisms are not able to fix the issue, try smoothing out your requests so
     * that large traffic bursts cannot overload the client, being more efficient
     * with the number of times you need to call AWS, or by increasing the number of
     * hosts sending requests.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String... args) throws Exception {

        // en0:        1693.58 KB/s         2863.68 KB/s         4557.27 KB/s
        
        // start time
        final long t0 = System.currentTimeMillis();
        
        final BatchWriter topicWriter = new BatchWriter(false, 2000);
        try {
            System.out.println("start");
            topicWriter.start();

            int rate = 7500;
            final RateLimiter rateLimiter = RateLimiter.create(rate); // per second
            for (int i = 0; i < 3600*rate ; ++i) {
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
        System.out.println(System.currentTimeMillis() - t0);

    }


}