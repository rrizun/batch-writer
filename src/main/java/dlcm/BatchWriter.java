package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
    private final AtomicReference<List<VoidFuture>> userRecordFutures = new AtomicReference<>(new ArrayList<>());

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
        synchronized (lock) {
            jsonWriter.beginArray();
        }
    }

    public void close() throws Exception {
        log("close");
        synchronized (lock) {
            jsonWriter.endArray();
            publishNow(); // does not block
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
                    publishNow().addListener(() -> {
                        setVoid(); // set future result
                    }, MoreExecutors.directExecutor());
                    jsonWriter.beginArray();
                }
            };
        }
    }

    /**
     * addUserRecord
     */
    public ListenableFuture<Void> addUserRecord(JsonElement jsonElement) throws Exception {
        synchronized (lock) {
            return new VoidFuture(){
                {
                    // requestMeter.mark(1);
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
    
                    userRecordFutures.get().add(this); // save future result
                }
            };
        }
    }

    private ListenableFuture<Void> publishNow() {
        log("publishNow", userRecordFutures.get().size());
        
        if (userRecordFutures.get().size()==0)
            return Futures.immediateVoidFuture();
        
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
                    final List<VoidFuture> copy = userRecordFutures.getAndSet(new ArrayList<>());

                    ListenableFuture<PublishResponse> listenableFuture = lf(snsClient.publish(publishRequest));
                    
                    requestMeter.mark(copy.size());
                    stats();
                    
                    ++busy;
                    listenableFuture.addListener(() -> {
                        synchronized (lock) {
                            try {
                                PublishResponse publishResponse = listenableFuture.get();
                                trace(publishResponse);
                                successMeter.mark(copy.size());
                                stats();
                                for (VoidFuture future : copy)
                                    future.setVoid();
                            } catch (Exception e) {
                                log(e);
                                failureMeter.mark(copy.size());
                                stats();
                                for (VoidFuture future : copy)
                                    future.setException(e);
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
                    // userRecordFutures.clear();

                } catch (Exception e) {
                    setException(e); // set publishNow future exception
                }
            }
        };
    }

    public static void main(String... args) throws Exception {

        // en0: 1693.58 KB/s 2863.68 KB/s 4557.27 KB/s

        // start time
        final long t0 = System.currentTimeMillis();
        
        //###TODO test multi-threaded writer here
        //###TODO test multi-threaded writer here
        //###TODO test multi-threaded writer here
        //###TODO test multi-threaded writer here
        //###TODO test multi-threaded writer here

        final ExecutorService executor = Executors.newFixedThreadPool(20);
        try {

            final BatchWriter topicWriter = new BatchWriter(false, 2000);
            try {
                System.out.println("start");
                topicWriter.start();
    
                final int rate = args.length > 0 ? Integer.parseInt(args[0]) : 17500;
                System.out.println("rate:" + rate);
                final RateLimiter rateLimiter = RateLimiter.create(rate); // per second
    
                List<ListenableFuture<Void>> sync = new CopyOnWriteArrayList<>();
                for (int i = 0; i < 5 * rate; ++i) {
                    executor.submit(()->{
                        int j = new Random().nextInt();
                        JsonObject userRecord = new JsonObject();
                        String key = Hashing.sha256().hashInt(j % rate).toString();
                        userRecord.addProperty("entityKey", key);
                        userRecord.addProperty("entityType", "/foo/bar/baz");
                        userRecord.addProperty("version", System.currentTimeMillis());
                        sync.add(topicWriter.addUserRecord(userRecord));
                        return Defaults.defaultValue(Void.class);
                    });
    
                    // rate limit
                    rateLimiter.acquire();
                }

                MoreExecutors.shutdownAndAwaitTermination(executor, Duration.ofMillis(Long.MAX_VALUE));
    
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

        } finally {
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