package dlcm;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import org.checkerframework.checker.units.qual.s;

import helpers.LogHelper;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

//###TODO rename to something better
//###TODO rename to something better
//###TODO rename to something better
class WriteItemFuture extends AbstractFuture<Void> {
//###TODO rename to something better
//###TODO rename to something better
//###TODO rename to something better
public boolean set(Void value) {
        return super.set(value);
    }
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }
}

public class DynamoWriter {

    // config
    private final String tableName;

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
    private final ExecutorService batchThread = Executors.newSingleThreadExecutor();

    // batch thread state
    // private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    private Map<WriteRequest, WriteItemFuture> writeRequests = new HashMap<>();
    // private ScheduledFuture<?> scheduledPublishFuture;

    private final MetricRegistry metrics = new MetricRegistry();

    private Meter wcuMeter() {
        return metrics.meter("wcu");
    }

    /**
     * ctor
     * 
     * @param tableName
     * @throws Exception
     */
    public DynamoWriter(String tableName) throws Exception {
        this.tableName = tableName;
    }

    public void start() throws Exception {
        wcuMeter().mark(0);
    }

    public void flush() {
        batchThread.execute(() -> {
            try {
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void close() throws Exception {
        flush();
        synchronized (this) {
            while (inFlight.get() != 0)
                wait();
        }
        dynamo.close();
        MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE));
    }

    public int writeRequestCount;

    /**
     * addUserRecord
     */
    public ListenableFuture<Void> addWriteRequest(Map<String, AttributeValue> item) {
        ++writeRequestCount;
        return new WriteItemFuture() {
            {
                batchThread.execute(() -> {
                    try {
                        writeRequests.put(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build(), this);
                        if (writeRequests.size() == 25)
                            publishNow();
                        set(Defaults.defaultValue(Void.class));
                    } catch (Exception e) {
                        setException(e);
                    }
                });
            }
        };
    }

    public final AtomicLong total = new AtomicLong();

    // this is run within the batchPool context
    private void publishNow() throws Exception {

        if (writeRequests.size() > 0) {

            doBatchWriteItem(writeRequests);

            writeRequests = new HashMap<>();
        }

    }

    private final AtomicInteger inFlight = new AtomicInteger();

    private void doBatchWriteItem(final Map<WriteRequest, WriteItemFuture> writeRequestAndWriteItemFutureMap) {

        //         Jun 16, 2020 4:14:42 AM io.netty.channel.DefaultChannelPipeline onUnhandledInboundException
        // WARNING: An exceptionCaught() event was fired, and it reached at the tail of the pipeline. It usually means the last handler in the pipeline did not handle the exception.
        // java.io.IOException: Request cancelled
        //         at software.amazon.awssdk.http.nio.netty.internal.FutureCancelHandler.exceptionCaught(FutureCancelHandler.java:43)

        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                //
                .requestItems(ImmutableMap.of(tableName, writeRequestAndWriteItemFutureMap.keySet()))
                //
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                //
                .build();

        inFlight.incrementAndGet();

        dynamo.batchWriteItem(batchWriteItemRequest)
                //
                .whenComplete((batchWriteItemResponse, throwable) -> {
                    try {
                        if (throwable!=null) {
                            for (WriteItemFuture future : writeRequestAndWriteItemFutureMap.values())
                                future.setException(throwable);
                        } else {
                            if (batchWriteItemResponse.sdkHttpResponse().isSuccessful()) {
                                for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity()) {
                                    Double consumedCapacityUnits = consumedCapacity.capacityUnits();
                                    total.addAndGet(consumedCapacityUnits.longValue());
                                    log("consumedCapacityUnits", consumedCapacityUnits);
                                    wcuMeter().mark(consumedCapacityUnits.longValue());
                                    log("wcuMeterMeanRate", Double.valueOf(wcuMeter().getMeanRate()).intValue());
        
                                    // process unprocessedItems
                                    Map<WriteRequest, WriteItemFuture> toBeRetried = new HashMap<>();
                                    if (batchWriteItemResponse.hasUnprocessedItems()) {
                                        for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                                                for (Entry<WriteRequest, WriteItemFuture> entry : writeRequestAndWriteItemFutureMap.entrySet()) {
                                                    if (!unprocessedItems.contains(entry.getKey()))
                                                        entry.getValue().set(Defaults.defaultValue(Void.class)); // success
                                                    else
                                                        toBeRetried.put(entry.getKey(), entry.getValue()); // retry
                                                }
                                        }
                                    }
                                    if (!toBeRetried.isEmpty())
                                        doBatchWriteItem(toBeRetried);
                                }
                            } else {
                                log(batchWriteItemResponse);
                            }
                        }
                    } finally {
                        inFlight.decrementAndGet();
                        synchronized (DynamoWriter.this) {
                            DynamoWriter.this.notify();
                        }
                    }
                })
                // //
                // .thenAccept(batchWriteItemResponse -> {
                // })
        //
        ;        

    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }
 
}