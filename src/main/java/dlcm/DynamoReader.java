package dlcm;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

import helpers.LogHelper;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

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
  private final DynamoDbAsyncClient dynamo = DynamoDbAsyncClient.builder()
  //
  //
  .build();

  private final Multimap<Map<String, AttributeValue>, GetItemFuture> allFutures = ArrayListMultimap.create();
  private final AtomicReference<Set<Map<String, AttributeValue>>> workingBatch = new AtomicReference<>(new HashSet<>());

  private int busy;
  private final Object lock = new Object();

  /**
   * ctor
   * 
   * @param tableName
   */
  public DynamoReader(String tableName) {
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
    synchronized (lock) {
      flush();
      while (busy > 0)
        lock.wait();
      }
    dynamo.close();
    log("allFutures", allFutures.size(), "workingBatch", workingBatch.get().size());
  }

  /**
   * flush
   * 
   * @return
   */
  public ListenableFuture<Void> flush() {
    log("flush");
    synchronized (lock) {
      return new AbstractFuture<Void>() {
        {
          sendBatchNow();
          Futures.allAsList(allFutures.values()).addListener(()->{
            set(Defaults.defaultValue(Void.class)); // set future result
          }, MoreExecutors.directExecutor());
        }
      };
    }
  }

  /**
   * addGetItemToBatch
   * 
   * @param key
   * @return
   */
  public ListenableFuture<Map<String, AttributeValue>> getItem(Map<String, AttributeValue> key) {
    trace("getItem", key);
    synchronized (lock) {
      return new GetItemFuture() {
        {
          workingBatch.get().add(key); // add to batch
          allFutures.put(key, this); // save this future result

          if (workingBatch.get().size() == 100)
            flush();
        }
      };
    }
  }

  /**
   * sendBatchNow
   */
  private void sendBatchNow() {
    if (!workingBatch.get().isEmpty()) {
      log("sendBatchNow");
      final Set<Map<String, AttributeValue>> inFlight = workingBatch.getAndSet(new HashSet<>());

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
          .build();

      ListenableFuture<BatchGetItemResponse> batchGetItemResponseFuture = lf(dynamo.batchGetItem(batchGetItemRequest));
      ++busy;
      batchGetItemResponseFuture.addListener(() -> {
        synchronized (lock) {
          try {
            BatchGetItemResponse batchGetItemResponse = batchGetItemResponseFuture.get();

            trace(batchGetItemResponse);
            // successMeter.mark(sentBatch.size());
            // stats("[<<<success<<<]");

            // failure 500
            if (batchGetItemResponse.hasUnprocessedKeys()) {
              for (KeysAndAttributes unprocessedKeys : batchGetItemResponse.unprocessedKeys().values()) {
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
                for (GetItemFuture future : allFutures.removeAll(key))
                  future.set(item);
              }
            }

            // success 404
            // "If a requested item does not exist, it is not returned in the result."
            for (Map<String, AttributeValue> key : inFlight) {
              for (GetItemFuture future : allFutures.removeAll(key))
                future.set(null);
            }
          } catch (Exception e) {
            log(e);
            e.printStackTrace();
            for (Map<String, AttributeValue> key : inFlight) {
              for (GetItemFuture future : allFutures.removeAll(key))
                future.setException(e);
            }
          } finally {
            --busy;
            lock.notifyAll();
          }
        }
      }, MoreExecutors.directExecutor());
    }
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

  // public static void main(String... args) throws Exception {

  //   final long t0 = System.currentTimeMillis();

  //   final DynamoReader dynamoReader = new DynamoReader("DlcmStack-TableCD117FA1-10BX86V213J7Z");
  //   dynamoReader.start();
  //   try {

  //     Map<String, AttributeValue> key = ImmutableMap.of("key",
  //         AttributeValue.builder().s("00e3d448ba79ee31b68784fd3890233ccf82c88e118984e7e129b921e87d7172").build());
      
  //     for (int i = 0; i < 5000; ++i) {
  //       ListenableFuture<Map<String, AttributeValue>> future = dynamoReader.getItem(key);
  //       future.addListener(()->{
  //         try {
  //           System.out.println(future.get());
  //         } catch (Exception e) {
  //           e.printStackTrace();
  //         }
  //       }, MoreExecutors.directExecutor());
  //     }

  //   } catch (Exception e) {
  //     e.printStackTrace();
  //   } finally {
  //     System.out.println("close[1]");
  //     dynamoReader.close();
  //     System.out.println("close[2]");

  //     log(System.currentTimeMillis()-t0, "ms");
  //   }

  // }


}