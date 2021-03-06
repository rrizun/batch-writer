package helpers;

import java.util.*;
import java.util.concurrent.*;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

import software.amazon.awssdk.core.client.config.*;
import software.amazon.awssdk.http.nio.netty.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

public class DynamoReader {

  private class GetItemFuture extends AbstractFuture<Map<String, AttributeValue>> {
    public boolean set(Map<String, AttributeValue> value) {
      return super.set(value);
    }
  
    public boolean setException(Throwable throwable) {
      return super.setException(throwable);
    }
  }

  private final String tableName;
  private final DynamoDbAsyncClient dynamo;

  private final Multimap<Map<String, AttributeValue>/* key */, GetItemFuture> workingSet = LinkedListMultimap.create();
  private final Multimap<Map<String, AttributeValue>/* key */, GetItemFuture> allFutures = LinkedListMultimap.create();

  private final LocalMeter requestMeter = new LocalMeter();
  private final LocalMeter successMeter = new LocalMeter();
  private final LocalMeter failureMeter = new LocalMeter();

  private final LocalMeter rcuMeter = new LocalMeter();

  /**
   * ctor
   * 
   * @param tableName
   */
  public DynamoReader(DynamoDbAsyncClient dynamo, String tableName) {
    log("ctor");
    this.dynamo = dynamo;
    this.tableName = tableName;
  }

  /**
   * getItem
   * 
   * @param key
   * @return
   */
  public ListenableFuture<Map<String, AttributeValue>> getItem(Map<String, AttributeValue> key) {
    requestMeter.mark(1);
    GetItemFuture getItemFuture = new GetItemFuture();
    workingSet.put(key, getItemFuture);
    if (workingSet.size()==100)
      batchGetItem().clear();
    return getItemFuture;
  }

  public ListenableFuture<Void> flush() {
    batchGetItem().clear();
    VoidFuture voidFuture = new VoidFuture();
    Futures.successfulAsList(allFutures.values()).addListener(()->{
      voidFuture.setVoid();
    }, MoreExecutors.directExecutor());
   return voidFuture;
  }

  /**
   * batchGetItem
   */
  private Multimap<Map<String, AttributeValue>, GetItemFuture> batchGetItem() {

    // dynamo item -> future
    Multimap<Map<String, AttributeValue>, GetItemFuture> copyOfKeys = ImmutableMultimap.copyOf(workingSet);

    // if (!inFlight.isEmpty())
    {
      try {
        // infer key schema
        Set<String> keySchema = new HashSet<>();
        for (Map<String, AttributeValue> key : copyOfKeys.keySet())
          keySchema.addAll(key.keySet());
  
        KeysAndAttributes keysAndAttributes = KeysAndAttributes.builder()
            //
            .keys(copyOfKeys.keySet())
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
                batchGetItemResponse.unprocessedKeys().values().forEach(unprocessedKey -> {
                  allFutures.removeAll(unprocessedKey).forEach(f -> {
                    failureMeter.mark(1);
                    f.setException(new Exception("UnprocessedKey")); // 500
                  });
                });
              }

              // success 200
              batchGetItemResponse.responses().values().forEach(responses->{
                responses.forEach(item->{
                  // infer key from item
                  Map<String, AttributeValue> key = Maps.asMap(keySchema, k -> item.get(k));
                  allFutures.removeAll(key).forEach(f->{
                    successMeter.mark(1);
                    f.set(item); // 200
                  });
                });
              });
  
              // success 404
              // "If a requested item does not exist, it is not returned in the result."
              copyOfKeys.keySet().forEach(key->{
                allFutures.removeAll(key).forEach(f->{
                  successMeter.mark(1);
                  f.set(null); // 404
                });
              });
            } catch (Exception e) {
              log(e);
              copyOfKeys.keySet().forEach(key->{
                allFutures.removeAll(key).forEach(f->{
                  failureMeter.mark(1);
                  f.setException(e); // 500
                });
              });
            } finally {
              stats("batchGetItemResponse");
            }
          } // synchronized
        }, MoreExecutors.directExecutor());
  
      } finally {
        stats("batchGetItemRequest");
      }
    }

    return workingSet;
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