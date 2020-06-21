package dlcm;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

import helpers.LogHelper;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

// at java.lang.Thread.run(Thread.java:748)
// Caused by: software.amazon.awssdk.core.exception.SdkClientException: Unable to execute HTTP request: Acquire operation took longer than the configured maximum time. This indicates that a request cannot get a connection from the pool within the specified maximum time. This can be due to high request rate.
// Consider taking any of the following actions to mitigate the issue: increase max connections, increase acquire timeout, or slowing the request rate.
// Increasing the max connections can increase client throughput (unless the network interface is already fully utilized), but can eventually start to hit operation system limitations on the number of file descriptors used by the process. If you already are fully utilizing your network interface or cannot further increase your connection count, increasing the acquire timeout gives extra time for requests to acquire a connection before timing out. If the connections doesn't free up, the subsequent requests will still timeout.
// If the above mechanisms are not able to fix the issue, try smoothing out your requests so that large traffic bursts cannot overload the client, being more efficient with the number of times you need to call AWS, or by increasing the number of hosts sending requests.
//         at software.amazon.awssdk.core.exception.SdkClientException$BuilderImpl.build(SdkClientException.java:98)
//         at software.amazon.awssdk.core.exception.SdkClientException.create(SdkClientException.java:43)
//         at software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper.setLastException(RetryableStageHelper.java:199)
//         at software.amazon.awssdk.core.internal.http.pipeline.stages.AsyncRetryableStage$RetryingExecutor.maybeRetryExecute(AsyncRetryableStage.java:143)
//         ... 18 more
// Caused by: java.lang.Throwable: Acquire operation took longer than the configured maximum time. This indicates that a request cannot get a connection from the pool within the specified maximum time. This can be due to high request rate.
// Consider taking any of the following actions to mitigate the issue: increase max connections, increase acquire timeout, or slowing the request rate.
// Increasing the max connections can increase client throughput (unless the network interface is already fully utilized), but can eventually start to hit operation system limitations on the number of file descriptors used by the process. If you already are fully utilizing your network interface or cannot further increase your connection count, increasing the acquire timeout gives extra time for requests to acquire a connection before timing out. If the connections doesn't free up, the subsequent requests will still timeout.
// If the above mechanisms are not able to fix the issue, try smoothing out your requests so that large traffic bursts cannot overload the client, being more efficient with the number of times you need to call AWS, or by increasing the number of hosts sending requests.
//         at software.amazon.awssdk.http.nio.netty.internal.NettyRequestExecutor.decorateException(NettyRequestExecutor.java:275)
//         at software.amazon.awssdk.http.nio.netty.internal.NettyRequestExecutor.handleFailure(NettyRequestExecutor.java:268)
//         ... 11 more
// Caused by: java.util.concurrent.TimeoutException: Acquire operation took longer then configured maximum time
//         at software.amazon.awssdk.http.nio.netty.internal.utils.BetterFixedChannelPool.<init>(...)(Unknown Source)
// java.util.concurrent.ExecutionException: software.amazon.awssdk.core.exception.SdkClientException: Unable to execute HTTP request: Acquire operation took longer than the configured maximum time. This indicates that a request cannot get a connection from the pool within the specified maximum time. This can be due to high request rate.
// Consider taking any of the following actions to mitigate the issue: increase max connections, increase acquire timeout, or slowing the request rate.
// Increasing the max connections can increase client throughput (unless the network interface is already fully utilized), but can eventually start to hit operation system limitations on the number of file descriptors used by the process. If you already are fully utilizing your network interface or cannot further increase your connection count, increasing the acquire timeout gives extra time for requests to acquire a connection before timing out. If the connections doesn't free up, the subsequent requests will still timeout.
// If the above mechanisms are not able to fix the issue, try smoothing out your requests so that large traffic bursts cannot overload the client, being more efficient with the number of times you need to call AWS, or by increasing the number of hosts sending requests.
//         at com.google.common.util.concurrent.AbstractFuture.getDoneValue(AbstractFuture.java:564)

/**
 * Consider taking any of the following actions to mitigate the issue: increase
 * max connections, increase acquire timeout, or slowing the request rate.
 * Increasing the max connections can increase client throughput (unless the
 * network interface is already fully utilized), but can eventually start to hit
 * operation system limitations on the number of file descriptors used by the
 * process. If you already are fully utilizing your network interface or cannot
 * further increase your connection count, increasing the acquire timeout gives
 * extra time for requests to acquire a connection before timing out. If the
 * connections doesn't free up, the subsequent requests will still timeout. If
 * the above mechanisms are not able to fix the issue, try smoothing out your
 * requests so that large traffic bursts cannot overload the client, being more
 * efficient with the number of times you need to call AWS, or by increasing the
 * number of hosts sending requests.
 */
class GetItemFuture extends AbstractFuture<Map<String, AttributeValue>> {
  public boolean set(Map<String, AttributeValue> value) {
    return super.set(value);
  }

  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }
}

public class DynamoReader {

  private final String tableName;

  private final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
  //
  .maxConcurrency(50*Runtime.getRuntime().availableProcessors())
  //
  // .maxPendingConnectionAcquires(10_000*Runtime.getRuntime().availableProcessors())
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

  private final AtomicReference<Set<Map<String, AttributeValue>>/* key */> workingBatch = new AtomicReference<>(new HashSet<>());
  private final Multimap<Map<String, AttributeValue>/* key */, GetItemFuture> allFutures = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());

  // private int busy;
  // private final Object busyCond = new Object();

  private final MyMeter requestMeter = new MyMeter();
  private final MyMeter successMeter = new MyMeter();
  private final MyMeter failureMeter = new MyMeter();

  private final MyMeter rcuMeter = new MyMeter();
  private final MyMeter unprocessedKeyMeter = new MyMeter();

  /**
   * ctor
   * 
   * @param tableName
   */
  public DynamoReader(String tableName) {
    log("ctor");
    this.tableName = tableName;
  }

  /**
   * start
   */
  public void start() throws Exception {
    log("start");
  }

  /**
   * close
   */
  public void close() throws Exception {
    log("close");
    stats("close");

    // flush();
    tryToBatch(workingBatch.get());

    synchronized (allFutures) {
      while (allFutures.size() > 0)
        allFutures.wait();
    }
    dynamo.close();
    stats("close");
  }

  // /**
  //  * flush
  //  * 
  //  * @return
  //  */
  // public ListenableFuture<Void> flush() {
  //   trace("flush");
  //   // synchronized (lock)
  //   {
  //     return new AbstractFuture<Void>() {
  //       {
  //         synchronized (workingBatch) {
  //           sendBatchNow(ImmutableSet.copyOf(workingBatch));
  //           workingBatch.clear();
  //         }
          
  //         //###TODO .VALUES NEEDS TO BE LOCKED HERE
  //         //###TODO .VALUES NEEDS TO BE LOCKED HERE
  //         //###TODO .VALUES NEEDS TO BE LOCKED HERE
  //         Futures.successfulAsList(allFutures.values()).addListener(()->{
  //           set(Defaults.defaultValue(Void.class)); // set future result
  //         }, MoreExecutors.directExecutor());
  //         //###TODO .VALUES NEEDS TO BE LOCKED HERE
  //         //###TODO .VALUES NEEDS TO BE LOCKED HERE
  //         //###TODO .VALUES NEEDS TO BE LOCKED HERE
  //       }
  //     };
  //   }
  // }

  /**
   * addGetItemToBatch
   * 
   * @param key
   * @return
   */
  public ListenableFuture<Map<String, AttributeValue>> getItem(Map<String, AttributeValue> key) {
    trace("getItem", key);
    // synchronized (lock)

    GetItemFuture future = new GetItemFuture();
    allFutures.put(key, future);

    Set<Map<String, AttributeValue>> tmp = ImmutableSet.of();
    synchronized (workingBatch) {
      workingBatch.get().add(key);
      if (workingBatch.get().size() == 100)
        tmp = workingBatch.getAndSet(new HashSet<>());
    }
    if (!tmp.isEmpty())
      tryToBatch(tmp);
    
    return future;
  }

  private Set<Map<String, AttributeValue>> addToBatch(Map<String, AttributeValue> key) {
    synchronized (workingBatch) {
      workingBatch.get().add(key);
      if (workingBatch.get().size() == 100)
        return workingBatch.getAndSet(new HashSet<>());
    }
    return ImmutableSet.of();
  }

  /**
   * tryToBatch
   */
  private void tryToBatch(Set<Map<String, AttributeValue>> inFlight) {

    

    if (!inFlight.isEmpty())
    {
      trace("sendBatchNow", inFlight.size());
      try {

        // final Set<Map<String, AttributeValue>> inFlight = workingBatch.getAndSet(new HashSet<>());

        // infer key schema
        Set<String> keySchema = new HashSet<>();
        for (Map<String, AttributeValue> key : inFlight)
          keySchema.addAll(key.keySet());
  
        KeysAndAttributes keysAndAttributes = KeysAndAttributes.builder()
            //
            .keys(inFlight)
            //
            .build();
  
        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder()
            //
            .requestItems(ImmutableMap.of(tableName, keysAndAttributes))
            //
            .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            //
            .build();
  
        ListenableFuture<BatchGetItemResponse> batchGetItemResponseFuture = lf(dynamo.batchGetItem(batchGetItemRequest));
        requestMeter.mark(inFlight.size());
        // ++busy;
        batchGetItemResponseFuture.addListener(() -> {
          // synchronized (lock)
          {
            try {
              BatchGetItemResponse batchGetItemResponse = batchGetItemResponseFuture.get();
  
              trace(batchGetItemResponse);
  
              for (ConsumedCapacity consumedCapacity : batchGetItemResponse.consumedCapacity())
                rcuMeter.mark(consumedCapacity.capacityUnits().longValue());
  
              // failure 500
              if (batchGetItemResponse.hasUnprocessedKeys()) {
                for (KeysAndAttributes unprocessedKeys : batchGetItemResponse.unprocessedKeys().values()) {
                  unprocessedKeyMeter.mark(unprocessedKeys.keys().size());
                  for (Map<String, AttributeValue> key : unprocessedKeys.keys()) {
                    for (GetItemFuture future : allFutures.removeAll(key))
                      future.setException(new Exception("UnprocessedKey"));
                  }
                }
              }
  
              // success 200
              for (List<Map<String, AttributeValue>> responses : batchGetItemResponse.responses().values()) {
                for (Map<String, AttributeValue> item : responses) {
                  // infer key
                  Map<String, AttributeValue> key = Maps.asMap(keySchema, k -> item.get(k));
                  for (GetItemFuture future : allFutures.removeAll(key)) {
                    successMeter.mark(1);
                    future.set(item);
                  }
                }
              }
  
              // success 404
              // "If a requested item does not exist, it is not returned in the result."
              for (Map<String, AttributeValue> key : inFlight) {
                for (GetItemFuture future : allFutures.removeAll(key)) {
                  successMeter.mark(1);
                  future.set(null);
                }
              }
            } catch (Exception e) {
              log(e);
              // e.printStackTrace();
              for (Map<String, AttributeValue> key : inFlight) {
                for (GetItemFuture future : allFutures.removeAll(key)) {
                  failureMeter.mark(1);
                  future.setException(e);
                }
              }
            } finally {
              synchronized(allFutures) {
                // --busy;
                allFutures.notifyAll();
              }
              stats("batchGetItemResponse");
            }
          } // synchronized
        }, MoreExecutors.directExecutor());
  
      } finally {
        stats("batchGetItemRequest");
      }
    }
  }

  private void stats(String s) {
    String inFlight = String.format("[%s]", allFutures.size());
    log(s, inFlight, "request", requestMeter, "success", successMeter, "failure", failureMeter);
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