package helpers;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.*;

import com.google.common.hash.*;
import com.google.common.io.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;
import com.google.gson.stream.*;
import com.spotify.futures.*;

import software.amazon.awssdk.core.client.config.*;
import software.amazon.awssdk.http.nio.netty.*;
import software.amazon.awssdk.services.sns.*;
import software.amazon.awssdk.services.sns.model.*;

public class BatchWriter {

    // config
    private final boolean compress; // true to use gzip compression

    private int busy; // in-flight
    private final Object lock = new Object();

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));

    private long nextFutureId;
    private final Map<Long, VoidFuture> allFutures = new HashMap<>();
    private final AtomicReference<Set<Long>> workingBatch = new AtomicReference<>(new HashSet<>());

    // private static final int DEFAULT_MAX_CONNECTIONS = 50;
    // private static final int DEFAULT_MAX_CONNECTION_ACQUIRES = 10_000;

    private final SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration.builder()
    //
    //
    .build();

    private final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
    //
    // .maxConcurrency(250)
    //
    ;

    private final ClientAsyncConfiguration clientAsyncConfiguration = ClientAsyncConfiguration.builder()
    //
    .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run)
    //
    .build();
  
    private final SnsAsyncClient snsClient = SnsAsyncClient.builder()
            //
            // .httpClientBuilder(httpClientBuilder)
            //
            .asyncConfiguration(clientAsyncConfiguration)
            //
            .build();

    private final String topicArn = "arn:aws:sns:us-east-2:743203956339:DlcmStack-DlcmInputTopic3467A01D-QKHDLR7RYNO7";

    private final MyMeter requestMeter = new MyMeter();
    private final MyMeter successMeter = new MyMeter();
    private final MyMeter failureMeter = new MyMeter();

    /**
     * ctor
     * 
     * @param compress
     * @throws Exception
     */
    public BatchWriter(boolean compress) throws Exception {
        log("ctor");
        this.compress = compress;
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
        stats("close");
    }

    public ListenableFuture<Void> flush() throws Exception {
        log("flush");
        synchronized (lock) {
            return new VoidFuture() {
                {
                    jsonWriter.endArray();
                    sendBatchNow();
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
                    workingBatch.get().add(id); // add to batch
                    allFutures.put(id, this); // save this future result
                }
            };
        }
    }

    private void sendBatchNow() throws Exception {
        trace("sendBatchNow", workingBatch.get().size());

        // if (userRecordFutures.get().size()==0)
        // return Futures.immediateVoidFuture();

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
            final Set<Long> sentBatch = workingBatch.getAndSet(new HashSet<>());
            // allFutures.putAll(copy);

            ListenableFuture<PublishResponse> listenableFuture = lf(snsClient.publish(publishRequest));

            requestMeter.mark(sentBatch.size());

            ++busy;
            listenableFuture.addListener(() -> {
                synchronized (lock) {
                    try {
                        PublishResponse publishResponse = listenableFuture.get();
                        trace(publishResponse);
                        successMeter.mark(sentBatch.size());
                        for (Long futureId : sentBatch) {
                            allFutures.remove(futureId).setVoid();
                        }
                    } catch (Exception e) {
                        log(e);
                        // e.printStackTrace();
                        failureMeter.mark(sentBatch.size());
                        for (Long futureId : sentBatch) {
                            allFutures.remove(futureId).setException(e);
                        }
                    } finally {
                        --busy;
                        lock.notifyAll(); // signal
                        stats("publishResponse");
                    }
                }
            }, MoreExecutors.directExecutor());

            // STEP 3 start new batch
            baos.reset();
            jsonWriter = new JsonWriter(new OutputStreamWriter(baos));

            stats("publishRequest");
    }

    private void stats(String s) {
        log(s, String.format("[%s]", busy), "request", requestMeter, "success", successMeter, "failure", failureMeter);
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