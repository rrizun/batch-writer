package helpers;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonWriter;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class TopicWriter {

    private final Object lock = new Object();

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));

    // private final List<VoidFuture> flushFutures = Lists.newCopyOnWriteArrayList();
    private final Multimap<JsonElement, VoidFuture> workingSet = ArrayListMultimap.create();
    private final Multimap<JsonElement, VoidFuture> allFutures = ArrayListMultimap.create();

    private final SnsAsyncClient snsClient = SnsAsyncClient.create();

    private final String topicArn;// = "arn:aws:sns:us-east-2:743203956339:DlcmStack-DlcmInputTopic3467A01D-QKHDLR7RYNO7";

    private final LocalMeter requestMeter = new LocalMeter();
    private final LocalMeter successMeter = new LocalMeter();
    private final LocalMeter failureMeter = new LocalMeter();

    /**
     * ctor
     * 
     * @param topicArn
     * @throws Exception
     */
    public TopicWriter(String topicArn) throws Exception {
        log("ctor");
        this.topicArn = topicArn;
        jsonWriter.beginArray();
    }

    public ListenableFuture<Void> flush() throws Exception {
        log("flush");
        synchronized (lock) {
            return new VoidFuture() {
                {
                    jsonWriter.endArray();
                    sendBatchNow(ImmutableMultimap.copyOf(workingSet)); // does not block
                    workingSet.clear();
                    jsonWriter.beginArray();

                    Futures.successfulAsList(allFutures.values()).addListener(()->{
                        log("flush", stats());
                        setVoid();
                    }, MoreExecutors.directExecutor());
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
                    requestMeter.mark(1);

                    String jsonValue = jsonElement.toString();

                    final int MAX_MSG_LEN = 256 * 1024; // sns/sqs 256KB

                    // will this exceed the max msg len?
                    // the -2 is for ',' and  ']'
                    if (baos.size() + jsonValue.length() > MAX_MSG_LEN - 2) {

                        // yes- publish now
                        jsonWriter.endArray();
                        sendBatchNow(ImmutableMultimap.copyOf(workingSet)); // does not block
                        workingSet.clear();
                        jsonWriter.beginArray();

                    }

                    jsonWriter.jsonValue(jsonValue);
                    jsonWriter.flush();

                    workingSet.put(jsonElement, this); // add to batch
                    allFutures.put(jsonElement, this); // save this future result
                }
            };
        }
    }

    private void sendBatchNow(ImmutableMultimap<JsonElement, VoidFuture> batch) throws Exception {
        new Object() {
            int busy; // in-flight
            List<Runnable> defer = new ArrayList<>();
            {
                synchronized (lock) {
                    try {
                        // STEP 1 close batch
                        jsonWriter.close();
    
                        // STEP 2 publish
                        String utf8 = new String(baos.toByteArray());
    
                        trace(utf8.length(), utf8.substring(0, Math.min(utf8.length(), 120)));
    
                        PublishRequest publishRequest = PublishRequest.builder()
                                //
                                .topicArn(topicArn)
                                //
                                .message(utf8)
                                //
                                .build();
    
                        trace(publishRequest.message().length());
    
                        ListenableFuture<PublishResponse> listenableFuture = lf(snsClient.publish(publishRequest));
                        ++busy;
                        listenableFuture.addListener(() -> {
                            synchronized (lock) {
                                --busy;
                                try {
                                    PublishResponse publishResponse = listenableFuture.get();
                                    trace(publishResponse);
                                    for (VoidFuture voidFuture : batch.values()) {
                                        // if (voidFuture.setVoid())
                                            successMeter.mark(1);
                                        defer.add(()->{
                                            voidFuture.setVoid();
                                        });
                                    }
                                } catch (Exception e) {
                                    doCatch(e);
                                } finally {
                                    doFinally();
                                }
                            }
                        }, MoreExecutors.directExecutor());
    
                        // STEP 3 start new batch
                        baos.reset();
                        jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
                    } catch (Exception e) {
                        doCatch(e);
                    } finally {
                        doFinally();
                    }
                }
            }
            void doCatch(Exception e) {
                log(e);
                e.printStackTrace();
                // setException(e);
                for (VoidFuture voidFuture : batch.values()) {
                    // if (voidFuture.setException(e))
                        failureMeter.mark(1);
                    defer.add(()->{
                        voidFuture.setException(e);
                    });
                }
            }
            void doFinally() {
                if (busy==0) {
                    --busy; // once
                    log("doFinally", stats());
                    for (Runnable runnable: defer)
                        runnable.run();
                    allFutures.values().removeAll(batch.values());
                    // setVoid();
                }
            }
        };
    }

    private String stats() {
        return new LogHelper(this).str("request", requestMeter, "success", successMeter, "failure", failureMeter);
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