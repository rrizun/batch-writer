package dlcm;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

import helpers.LogHelper;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

class WriteRequestFuture extends AbstractFuture<Void> {
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
            .maxConcurrency(50*Runtime.getRuntime().availableProcessors())
    // //
    // .maxPendingConnectionAcquires(10_000)
    //
    ;

    private final ClientAsyncConfiguration clientAsyncConfiguration = ClientAsyncConfiguration.builder()
    //
    .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, MoreExecutors.directExecutor())
    //
    .build();
  
    private final DynamoDbAsyncClient dynamo = DynamoDbAsyncClient.builder()
            //
            .httpClientBuilder(httpClientBuilder)
            //
            .asyncConfiguration(clientAsyncConfiguration)
            //
            .build();

    private int busy;
    private final Object lock = new Object();

    private final Map<WriteRequest, WriteRequestFuture> workingBatch = new HashMap<>();
    // private final Map<Map<String, AttributeValue>/*keyOrItem*/, WriteRequestFuture> workingBatch = new HashMap<>();

    // private final Map<WriteRequest, WriteRequestFuture> allFutures = new HashMap<>();

    private final MyMeter requestMeter = new MyMeter();
    private final MyMeter successMeter = new MyMeter();
    private final MyMeter failureMeter = new MyMeter();
  
    private final MyMeter wcuMeter = new MyMeter();
    private final MyMeter unprocessedItemMeter = new MyMeter();
  
    /**
     * ctor
     * 
     * @param tableName
     */
    public DynamoWriter(String tableName) {
        log("ctor");
        this.tableName = tableName;
    }

    public void start() throws Exception {
        log("start");
    }

    public void close() throws Exception {
        log("close");
        flush();
        synchronized (lock) {
            while (busy > 0) {
                lock.wait();
            }
        }
        dynamo.close();
    }

    public void flush() throws Exception {
        log("flush");
        synchronized (lock) {
            doBatchWriteItem(ImmutableMap.copyOf(workingBatch));
            workingBatch.clear();
        }
    }

    /**
     * addPutItem
     * 
     * @param item
     * @return
     */
    public ListenableFuture<Void> addPutItem(Map<String, AttributeValue> item) {
        return new WriteRequestFuture() {
            {
                ImmutableMap<WriteRequest, WriteRequestFuture> tmp = ImmutableMap.of();
                synchronized (lock) {
                    workingBatch.put(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build(), this);
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    if (workingBatch.size() == 25) {
                        tmp = ImmutableMap.copyOf(workingBatch);
                        workingBatch.clear();
                    }
                }
                if (!tmp.isEmpty())
                    doBatchWriteItem(tmp);

            }
        };
    }

    /**
     * addDeleteItem
     * 
     * @param key
     * @return
     */
    public ListenableFuture<Void> addDeleteItem(Map<String, AttributeValue> key) {
        return new WriteRequestFuture() {
            {
                synchronized (lock) {
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    workingBatch.put(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build(), this);
                    if (workingBatch.size() == 25) {
                        doBatchWriteItem(ImmutableMap.copyOf(workingBatch));
                        workingBatch.clear();
                    }
                }
            }
        };
    }

    private void doBatchWriteItem(ImmutableMap<WriteRequest, WriteRequestFuture> requestItems) {

        if (requestItems.size()==0)
            return;

        // log("doBatchWriteItem", requestItems.size());

        try {

            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    //
                    .requestItems(ImmutableMap.of(tableName, requestItems.keySet()))
                    //
                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    //
                    .build();

            trace(batchWriteItemRequest);

            // "BatchWriteItem does not return deleted items in the response."

            // If one or more of the following is true, DynamoDB rejects the entire batch
            // write operation:
            // "You try to perform multiple operations on the same item in the same
            // BatchWriteItem request.
            // For example, you cannot put and delete the same item in the same
            // BatchWriteItem request"

            ListenableFuture<BatchWriteItemResponse> responseFuture = lf(dynamo.batchWriteItem(batchWriteItemRequest));

            requestMeter.mark(requestItems.size());

            responseFuture.addListener(() -> {
                // synchronized (lock)
                {
                    
                    try {
                        BatchWriteItemResponse batchWriteItemResponse = responseFuture.get();
    
                        trace(batchWriteItemResponse);
    
                        for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity())
                            wcuMeter.mark(consumedCapacity.capacityUnits().longValue());
    
                        // failure 500
                        if (batchWriteItemResponse.hasUnprocessedItems()) {
                            for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                                for (WriteRequest writeRequest : unprocessedItems) {
                                    if (requestItems.get(writeRequest).setException(new Exception("UnprocessedItem")))
                                        failureMeter.mark(1);
                                }
                            }
                        }
    
                        // success 200
                        for (WriteRequestFuture future : requestItems.values()) {
                            if (future.set(Defaults.defaultValue(Void.class)))
                                successMeter.mark(1);
                        }
    
                    } catch (Exception e) {
                        log(e);
                        failureMeter.mark(requestItems.size());
                        for (WriteRequestFuture future : requestItems.values())
                            future.setException(e);
                    } finally {
                        synchronized (lock) {
                            --busy;
                            lock.notifyAll();
                        }
                        stats("batchWriteItemResponse");
                    }
                }
            }, MoreExecutors.directExecutor());

        } finally {
            synchronized (lock) {
                ++busy;
            }
            stats("batchWriteItemRequest");
        }

    }

    private void stats(String s) {
        String zzz = String.format("[%s]", busy);
        log(s, zzz, "request", requestMeter, "success", successMeter, "failure", failureMeter, "wcu", wcuMeter);
        // log(s, "request", requestMeter, "success", successMeter, "failure", failureMeter, "inFlight", allFutures.size(), "rcu", rcuMeter, "unprocessedKey", unprocessedKeyMeter);
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