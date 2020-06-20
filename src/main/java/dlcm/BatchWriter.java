package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Defaults;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
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

class VoidFuture extends AbstractFuture<Void> {
    public boolean setVoid() {
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

    private final Object lock = new Object();

    private int busy; // in-flight

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    private final AtomicReference<Set<Long>> userRecordFutures = new AtomicReference<>(new HashSet<>());

    private long nextFutureId;
    private final Map<Long, VoidFuture> allFutures = new HashMap<>();

    // private static final int DEFAULT_MAX_CONNECTIONS = 50;
    // private static final int DEFAULT_MAX_CONNECTION_ACQUIRES = 10_000;

    private final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
    //
    .maxConcurrency(250)
    //
    ;

    private final SnsAsyncClient snsClient = SnsAsyncClient.builder()
            //
            .httpClientBuilder(httpClientBuilder)
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
        synchronized (lock) {
            jsonWriter.beginArray();
        }
    }

    public void close() throws Exception {
        log("close");
        synchronized (lock) {
            jsonWriter.endArray();
            sendBatchNow(); // does not block
            while (busy > 0)
                lock.wait();
        }
        snsClient.close();
    }

    public ListenableFuture<Void> flush() throws Exception {
        log("flush");
        synchronized (lock) {
            return new VoidFuture() {
                {
                    jsonWriter.endArray();
                    sendBatchNow().addListener(() -> {
                        // setVoid(); // set future result
                    }, MoreExecutors.directExecutor());

                    Futures.allAsList(allFutures.values()).addListener(()->{
                        setVoid(); // set future result
                    }, MoreExecutors.directExecutor());
                    jsonWriter.beginArray();
                }
            };
        }
    }

    /**
     * addToBatch
     */
    public ListenableFuture<Void> addToBatch(JsonElement jsonElement) throws Exception {
        synchronized (lock) {
            return new VoidFuture() {
                {
                    // requestMeter.mark(1);
                    String jsonValue = jsonElement.toString();

                    final int MAX_MSG_LEN = 256 * 1024; // sns/sqs 256KB

                    // will this exceed the max msg len?
                    // the -2 is for ',' and  ']'
                    if (baos.size() + jsonValue.length() > MAX_MSG_LEN - 2) {

                        // yes- publish now
                        jsonWriter.endArray();
                        sendBatchNow(); // does not block
                        jsonWriter.beginArray();

                    }

                    jsonWriter.jsonValue(jsonValue);
                    jsonWriter.flush();

                    long id = ++nextFutureId;
                    userRecordFutures.get().add(id); // add to batch
                    allFutures.put(id, this); // save this future result
                }
            };
        }
    }

    private ListenableFuture<Void> sendBatchNow() {
        trace("sendBatchNow", userRecordFutures.get().size());

        // if (userRecordFutures.get().size()==0)
        // return Futures.immediateVoidFuture();

        return new AbstractFuture<Void>() {
            {
                try {

                    // STEP 1 close batch
                    jsonWriter.close();

                    // STEP 2 publish
                    String utf8 = new String(baos.toByteArray());
                    if (compress) {
                        ByteArrayOutputStream tmp = new ByteArrayOutputStream();
                        try (OutputStream out = new GZIPOutputStream(tmp)) {
                            ByteStreams.copy(new ByteArrayInputStream(baos.toByteArray()), out);
                        }
                        utf8 = BaseEncoding.base64().encode(tmp.toByteArray());
                    }
                    // log(baos.size(), utf8[0].length());

                    trace(utf8.length(), utf8.substring(0, Math.min(utf8.length(), 120)));

                    PublishRequest publishRequest = PublishRequest.builder()
                            //
                            .topicArn(topicArn)
                            //
                            .message(utf8)
                            //
                            .build();

                    trace(publishRequest.message().length());

                    // final int finalUserRecordBatchCount = userRecordBatchCount; // take snapshot
                    final Set<Long> copy = userRecordFutures.getAndSet(new HashSet<>());
                    // allFutures.putAll(copy);

                    ListenableFuture<PublishResponse> listenableFuture = lf(snsClient.publish(publishRequest));

                    requestMeter.mark(copy.size());
                    stats("[>>>request>>>]");

                    ++busy;
                    listenableFuture.addListener(() -> {
                        synchronized (lock) {
                            try {
                                PublishResponse publishResponse = listenableFuture.get();
                                trace(publishResponse);
                                successMeter.mark(copy.size());
                                stats("[<<<success<<<]");
                                for (Long futureId : copy) {
                                    allFutures.remove(futureId).setVoid();
                                }
                            } catch (Exception e) {
                                log(e);
                                // e.printStackTrace();
                                log("publishRequestMessageLength", publishRequest.message().length());
                                failureMeter.mark(copy.size());
                                stats("[<<<failure<<<]");
                                for (Long futureId : copy) {
                                    allFutures.remove(futureId).setException(e);
                                }
                            } finally {
                                --busy;
                                lock.notifyAll(); // signal
                                set(Defaults.defaultValue(Void.class)); // set publishNow future result
                            }
                        }
                    }, MoreExecutors.directExecutor());

                    // STEP 3 start new batch
                    baos.reset();
                    jsonWriter = new JsonWriter(new OutputStreamWriter(baos));

                } catch (Exception e) {
                    setException(e); // set publishNow future exception
                }
            }
        };
    }

    public static void main(String... args) throws Exception {

        // start time
        final long t0 = System.currentTimeMillis();
        final BatchWriter topicWriter = new BatchWriter(false, 2000);

        // topicWriter.start();
        // topicWriter.flush().get();
        // topicWriter.close();
        // if (new Random().nextInt()!=0)
        //     System.exit(0);

        try {
            System.out.println("start");
            topicWriter.start();

            final long rate = args.length > 0 ? Long.parseLong(args[0]) : 7500;
            System.out.println("rate:" + rate);
            final RateLimiter rateLimiter = RateLimiter.create(rate); // per second

            // List<ListenableFuture<Void>> sync = new CopyOnWriteArrayList<>();
            long seconds = 25;
            for (long i = 1; i <= seconds * rate; ++i) {
                JsonObject userRecord = new JsonObject();
                String key = Hashing.sha256().hashLong(i % rate).toString();

                byte[] bytes = new byte[new Random().nextInt(16)];
                new SecureRandom().nextBytes(bytes);
                key = BaseEncoding.base64().encode(bytes).toString();

                userRecord.addProperty("entityKey", key);
                userRecord.addProperty("entityType", "/foo/bar/baz");
                userRecord.addProperty("version", System.currentTimeMillis());
                
                ListenableFuture<Void> f = topicWriter.addToBatch(userRecord);
                // sync.add(f);

                // rate limit
                rateLimiter.acquire();

                // periodic checkpoint
                // if (i % (2*seconds) == 0)
                // {
                //     System.out.println("call flush.get[1]");
                //     // topicWriter.flush();
                //     System.out.println("call flush.get[2]");
                //     // Futures.allAsList(sync).get();
                //     // sync.clear();
                // }
            }

            // System.out.println("call flush.get[1]");
            // topicWriter.flush().get();
            // System.out.println("call flush.get[2]");

        } finally {
            System.out.println("close[1]");
            topicWriter.close();
            System.out.println("close[2]");
        }

        // finish time
        System.out.println((System.currentTimeMillis() - t0) + "ms");

    }

    private void stats(String s) {
        log(s, "request", requestMeter, "success", successMeter, "failure", failureMeter);
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