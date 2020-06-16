package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import helpers.LogHelper;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoWriter {

    // config
    private final String tableName;
    private final long lingerMs;

    private final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
    //
        .maxConcurrency(10000)
        // //
        // .maxPendingConnectionAcquires(10_000)
        //
        ;


    private final DynamoDbAsyncClient dynamo = DynamoDbAsyncClient.builder()
    //
    .httpClientBuilder(httpClientBuilder)
    //
    .build();


    // batch thread
    private final ScheduledExecutorService batchThread = Executors.newSingleThreadScheduledExecutor();
    
    // batch thread state
    // private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    private List<WriteRequest> writeRequests = new ArrayList<>();
    // private ScheduledFuture<?> scheduledPublishFuture;


    // publish threads
    @Deprecated
    private final ExecutorService publishPool = Executors.newCachedThreadPool();

    private final MetricRegistry metrics = new MetricRegistry();

    private Meter wcuMeter() { return metrics.meter("wcu"); }

    /**
     * ctor
     * 
     * @param tableName
     * @param lingerMs
     * @throws Exception
     */
    public DynamoWriter(String tableName, long lingerMs) throws Exception {
        this.tableName = tableName;
        this.lingerMs = lingerMs;
    }

    public void start() throws Exception {
        wcuMeter().mark(0);
        batchThread.execute(() -> {
            try {
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        schedulePublish();
    }

    public void close() throws Exception {
        batchThread.execute(()->{
            try {
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        synchronized (this) {
            while(inFlight.get()!=0)
                wait();
        }

        MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE));
        MoreExecutors.shutdownAndAwaitTermination(publishPool, Duration.ofMillis(Long.MAX_VALUE));
    }

    public int writeRequestCount;

    /**
     * addUserRecord
     */
    public void addWriteRequest(Map<String, AttributeValue> item) {
        ++writeRequestCount;
        batchThread.execute(() -> {
            try {
                writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());
                if (writeRequests.size() == 25)
                    publishNow();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public final AtomicLong total = new AtomicLong();
    
    // this is run within the batchPool context
    private void publishNow() throws Exception {
        // cancel scheduled publish
        cancelScheduledPublish();

        if (writeRequests.size() > 0) {

            doBatchWriteItem(ImmutableMap.of(tableName, writeRequests));

            // BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
            //         //
            //         .requestItems()
            //         //
            //         .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            //         //
            //         .build();

            // publishPool.execute(() -> {
            //     try {

            //         BatchWriteItemResponse batchWriteItemResponse = dynamo.batchWriteItem(batchWriteItemRequest).get();

            //         if (batchWriteItemResponse.hasUnprocessedItems()) {
            //             for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
            //                 for (WriteRequest writeRequest : unprocessedItems)
            //                     addWriteRequest(writeRequest.putRequest().item()); // ### TODO ugly
            //             }
            //         }

            //         Double consumedCapacityUnits = batchWriteItemResponse.consumedCapacity().iterator().next().capacityUnits();
            //         total.addAndGet(consumedCapacityUnits.longValue());
            //         log("consumedCapacityUnits", consumedCapacityUnits);
            //         wcuMeter.mark(consumedCapacityUnits.longValue());
            //         log("wcuMeterMeanRate", Double.valueOf(wcuMeter.getMeanRate()).intValue());

            //     } catch (Exception e) {
            //         e.printStackTrace();;
            //         System.exit(-1);
            //         throw new RuntimeException(e);
            //     }
            // });

            writeRequests = new ArrayList<>();
        }

        // reschedule scheduled publish
        schedulePublish();
    }

    private final AtomicInteger inFlight = new AtomicInteger();

    private void doBatchWriteItem(Map<String, ? extends Collection<WriteRequest>> requestItems) {

        //         Jun 16, 2020 4:14:42 AM io.netty.channel.DefaultChannelPipeline onUnhandledInboundException
        // WARNING: An exceptionCaught() event was fired, and it reached at the tail of the pipeline. It usually means the last handler in the pipeline did not handle the exception.
        // java.io.IOException: Request cancelled
        //         at software.amazon.awssdk.http.nio.netty.internal.FutureCancelHandler.exceptionCaught(FutureCancelHandler.java:43)

        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                //
                .requestItems(requestItems)
                //
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                //
                .build();

        inFlight.incrementAndGet();

        dynamo.batchWriteItem(batchWriteItemRequest)
                //
                .thenAccept(batchWriteItemResponse -> {
                    if (batchWriteItemResponse.sdkHttpResponse().isSuccessful()) {
                        for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity()) {
                            Double consumedCapacityUnits = consumedCapacity.capacityUnits();
                            total.addAndGet(consumedCapacityUnits.longValue());
                            log("consumedCapacityUnits", consumedCapacityUnits);
                            wcuMeter().mark(consumedCapacityUnits.longValue());
                            log("wcuMeterMeanRate", Double.valueOf(wcuMeter().getMeanRate()).intValue());
                            if (batchWriteItemResponse.hasUnprocessedItems()) {
                                if (!batchWriteItemResponse.unprocessedItems().isEmpty())
                                    doBatchWriteItem(batchWriteItemResponse.unprocessedItems());
                            }
                        }
                    } else {
                        log(batchWriteItemResponse);
                    }
                })
                //
                .whenComplete((a, b) -> {
                    inFlight.decrementAndGet();
                    synchronized (DynamoWriter.this) {
                        DynamoWriter.this.notify();
                    }
                })
        //
        ;        

    }

    private void schedulePublish() {
        // scheduledPublishFuture = batchThread.schedule(()->{
        //     try {
        //         publishNow(); // does not block
        //     } catch (Exception e) {
        //         throw new RuntimeException(e);
        //     }
        // }, lingerMs, TimeUnit.MILLISECONDS);
    }

    private void cancelScheduledPublish() {
        // scheduledPublishFuture.cancel(true);
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }
 
}