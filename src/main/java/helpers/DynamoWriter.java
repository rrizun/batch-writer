package helpers;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

import software.amazon.awssdk.core.client.config.*;
import software.amazon.awssdk.http.nio.netty.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

public class DynamoWriter {

    // config
    private final String tableName;
    private final DynamoDbAsyncClient dynamo;

    // private final Object lock = new Object();
    private AtomicInteger busy = new AtomicInteger();

    private final Set<WriteRequest> workingSet = new HashSet<>(); // synchronized
    private final Map<WriteRequest, VoidFuture> allFutures = new ConcurrentHashMap<>();

    private final LocalMeter requestMeter = new LocalMeter();
    private final LocalMeter successMeter = new LocalMeter();
    private final LocalMeter failureMeter = new LocalMeter();

    private final LocalMeter wcuMeter = new LocalMeter();

    /**
     * ctor
     * 
     * @param tableName
     */
    public DynamoWriter(String tableName, DynamoDbAsyncClient dynamo) {
        log("ctor");
        this.tableName = tableName;
        this.dynamo = dynamo;
    }

    /**
     * start
     * 
     * @return
     * @throws Exception
     */
    public ListenableFuture<Void> start() throws Exception {
        log("start");
        return Futures.immediateVoidFuture();
    }

                // /**
                //  * close
                //  * 
                //  * @return
                //  * @throws Exception
                //  */
                // public ListenableFuture<Void> close() throws Exception {
                //     stats("close[1]");
                //     return new AbstractFuture<Void>() {
                //         {
                //             // synchronized (lock)
                //             {
                //                 doBatchWriteItem(addAndBatch(null));
                //                 Futures.allAsList(allFutures.values()).addListener(() -> {
                //                     // synchronized (lock)
                //                     {
                //                         try {
                //                             dynamo.close();
                //                         } catch (Exception e) {
                //                             setException(e);
                //                         } finally {
                //                             stats("close[2]");
                //                             set(Defaults.defaultValue(Void.class));
                //                         }
                //                     }
                //                 }, MoreExecutors.directExecutor());
                //             }
                //         }
                //     };
                // }

    /**
     * flush
     * 
     * @return
     * @throws Exception
     */
    public ListenableFuture<Void> flush() throws Exception {
        log("flush");
        return new AbstractFuture<Void>() {
            {
                // synchronized (lock)
                {
                    doBatchWriteItem(addAndBatch(null));
                    Futures.successfulAsList(allFutures.values()).addListener(() -> {
                        // synchronized (lock)
                        {
                            set(Defaults.defaultValue(Void.class));
                        }
                    }, MoreExecutors.directExecutor());
                }
            }
        };
    }

    /**
     * addPutItem
     * 
     * @param item
     * @return
     */
    public ListenableFuture<Void> addPutItem(Map<String, AttributeValue> item) {
        return new VoidFuture() {
            {
                // synchronized (lock)
                {
                    WriteRequest writeRequest = WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
                    allFutures.put(writeRequest, this);
                    doBatchWriteItem(addAndBatch(writeRequest));
                }
            }
        };
    }

    // /**
    //  * addDeleteItem
    //  * 
    //  * @param key
    //  * @return
    //  */
    // public ListenableFuture<Void> addDeleteItem(Map<String, AttributeValue> key) {
    //     return new VoidFuture() {
    //         {
    //             synchronized (lock) {
    //                 WriteRequest writeRequest = WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build();
    //                 allFutures.put(writeRequest, this);
    //                 doBatchWriteItem(addAndBatch(writeRequest));
    //             }
    //         }
    //     };
    // }

    private ImmutableSet<WriteRequest> addAndBatch(WriteRequest writeRequest) {
        synchronized (workingSet)
        {
            if (writeRequest != null)
                workingSet.add(writeRequest);
            if (workingSet.size() == 25) {
                ImmutableSet<WriteRequest> tmp = ImmutableSet.copyOf(workingSet);
                workingSet.clear();
                return tmp;
            }
        }
        return ImmutableSet.of();
    }

    private void doBatchWriteItem(final ImmutableSet<WriteRequest> requestItems) {

        if (requestItems.size() == 0)
            return;

        // log("doBatchWriteItem", requestItems.size());

        try {

            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    //
                    .requestItems(ImmutableMap.of(tableName, requestItems))
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

            busy.incrementAndGet();
            responseFuture.addListener(() -> {
                busy.decrementAndGet();
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
                                    failureMeter.mark(1);
                                    allFutures.remove(writeRequest).setException(new Exception("Oof!"));
                                }
                            }
                        }

                        // success 200
                        for (WriteRequest writeRequest : requestItems) {
                            VoidFuture future = allFutures.remove(writeRequest);
                            if (future!=null) {
                                successMeter.mark(1);
                                future.set();
                            }

                        }

                        stats("batchWriteItemResponse");

                    } catch (Exception e) {
                        log(e);
                        for (WriteRequest writeRequest : requestItems) {
                            failureMeter.mark(1);
                            allFutures.remove(writeRequest).setException(e);
                        }
                    }
                }
            }, MoreExecutors.directExecutor());

        } finally {
            stats("batchWriteItemRequest");
        }

    }

    private void stats(String s) {
        String state = String.format("[%s/%s]", busy, allFutures.size());
        log(s, state, "request", requestMeter, "success", successMeter, "failure", failureMeter, "wcu", wcuMeter);
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