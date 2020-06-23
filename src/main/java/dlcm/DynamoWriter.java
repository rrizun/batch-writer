package dlcm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
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
            // .maxConcurrency(50*Runtime.getRuntime().availableProcessors())
    // //
    // .maxPendingConnectionAcquires(10_000)
    //
    ;

    private final ClientAsyncConfiguration clientAsyncConfiguration = ClientAsyncConfiguration.builder()
    //
    // .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, MoreExecutors.directExecutor())
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

    private final Set<WriteRequest> workingSet = new HashSet<>();
    private final Map<WriteRequest, WriteRequestFuture> allFutures = new HashMap<>();

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

    public ListenableFuture<Void> start() throws Exception {
        log("start");
        return Futures.immediateVoidFuture();
    }

    public ListenableFuture<Void> close() throws Exception {
        stats("close");
        return new AbstractFuture<Void>() {
          {
            synchronized (lock) {
              flush().addListener(()->{
                synchronized (lock) {
                    Futures.allAsList(allFutures.values()).addListener(() -> {
                        synchronized (lock) {
                          try {
                              dynamo.close();
                              stats("close");
                              set(Defaults.defaultValue(Void.class));
                            } catch (Exception e) {
                                log(e);
                                  setException(e);
                            }
                      }
                    }, MoreExecutors.directExecutor());
                }
              }, MoreExecutors.directExecutor());
            }
          }
        };

        // sendBatchNow();
        // Futures.allAsList(allFutures.values()).addListener(()->{
        //     setVoid(); // set future result
        // }, MoreExecutors.directExecutor());

    }

    public ListenableFuture<Void> flush() throws Exception {
        log("flush");
        return new AbstractFuture<Void>(){
            {
                synchronized (lock) {
                    doBatchWriteItem(copyAndClear(workingSet));
                    Futures.allAsList(allFutures.values()).addListener(()->{
                        synchronized (lock) {
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
        return new WriteRequestFuture() {
            {
                synchronized (lock) {
                    WriteRequest writeRequest = WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
                    
                    if (allFutures.containsKey(writeRequest))
                        log("HOLY CRAP", writeRequest);
                    
                    allFutures.put(writeRequest, this);
                    
                    if (workingSet.contains(writeRequest))
                        log("HOLY CRAP", writeRequest);
                    
                    workingSet.add(writeRequest);
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    if (workingSet.size() == 25)
                        doBatchWriteItem(copyAndClear(workingSet));
                }
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
                //###TODO THIS LOCK IS TOO COARSE GRAINED
                //###TODO THIS LOCK IS TOO COARSE GRAINED
                //###TODO THIS LOCK IS TOO COARSE GRAINED
                synchronized (lock) {

                    WriteRequest writeRequest = WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build();

                    allFutures.put(writeRequest, this);
                //###TODO THIS LOCK IS TOO COARSE GRAINED
                //###TODO THIS LOCK IS TOO COARSE GRAINED
                //###TODO THIS LOCK IS TOO COARSE GRAINED
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    //###TODO also commit batch if duplicate keys
                    workingSet.add(writeRequest);
                    if (workingSet.size() == 25)
                        doBatchWriteItem(copyAndClear(workingSet));
                }
            }
        };
    }

    private ImmutableSet<WriteRequest> copyAndClear(Set<WriteRequest> in) {
        ImmutableSet<WriteRequest> result = ImmutableSet.copyOf(in);
        in.clear();
        return result;
    }

    private void doBatchWriteItem(final ImmutableSet<WriteRequest> requestItems) {

        if (requestItems.size()==0)
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

            ++busy;
            responseFuture.addListener(() -> {
                synchronized (lock) {
                    --busy;
                    try {
                        BatchWriteItemResponse batchWriteItemResponse = responseFuture.get();
    
                        trace(batchWriteItemResponse);
    
                        for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity())
                            wcuMeter.mark(consumedCapacity.capacityUnits().longValue());

                        // failure 500
                        if (batchWriteItemResponse.hasUnprocessedItems()) {
                            for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                                for (WriteRequest writeRequest : unprocessedItems)
                                    failureMeter.mark(1);
                            }
                        }
    
                        // success 200
                        for (WriteRequest writeRequest : requestItems) {
                            if (allFutures.containsKey(writeRequest))
                                successMeter.mark(1);
                        }

                        stats("batchWriteItemResponse");

                        // notify listeners
                        if (batchWriteItemResponse.hasUnprocessedItems()) {
                            for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                                for (WriteRequest writeRequest : unprocessedItems)
                                    allFutures.remove(writeRequest).setException(new Exception("UnprocessedItem"));
                            }
                        }
                        for (WriteRequest writeRequest : requestItems) {
                            if (allFutures.containsKey(writeRequest))
                                allFutures.remove(writeRequest).set(Defaults.defaultValue(Void.class));
                        }
    
                    } catch (Exception e) {
                        log(e);
                        for (WriteRequest writeRequest : requestItems) {
                            if (allFutures.containsKey(writeRequest))
                                failureMeter.mark(1);
                        }
                        for (WriteRequest writeRequest : requestItems) {
                            if (allFutures.containsKey(writeRequest))
                                allFutures.remove(writeRequest).setException(e);
                        }
                    } finally {
                        // for (WriteRequestFuture future : success) {
                        //     future.set(Defaults.defaultValue(Void.class));
                        // }
                        // for (WriteRequestFuture future : failure) {
                        //     future.setException(e);
                            
                        // }
                        // synchronized (lock) {
                        //     --busy;
                        //     lock.notifyAll();
                        // }
                    }
                }
            }, MoreExecutors.directExecutor());

        } finally {
            // synchronized (lock) {
            //     ++busy;
            // }
            // stats("batchWriteItemRequest");
        }

    }

    private void stats(String s) {
        log(s, String.format("[%s]", busy), "request", requestMeter, "success", successMeter, "failure", failureMeter, "wcu", wcuMeter);
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