package dlcm;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonObject;

import helpers.LogHelper;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class Main {

  public static void main(String... args) throws Exception {
    new Main().run();
  }

  private final String tableName = "DlcmStack-TableCD117FA1-GVAWLZWY3HBE";

  private final RateLimiter wcuLimiter = RateLimiter.create(10000);

  private final DynamoDbClient dynamo = DynamoDbClient.create();
  private final ExecutorService pool = Executors.newCachedThreadPool();
  // private final DynamoDbAsyncClient dynamo = DynamoDbAsyncClient.create();

  private final MetricRegistry metrics = new MetricRegistry();

  private final Meter wcuMeter = metrics.meter("wcu");

  public void run() throws Exception {
    log("run");

    // create items
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (int i = 0; i < 100000; ++i)
      items.add(createItem(i));

    // create dynamo write requests
    List<WriteRequest> writeRequests = new ArrayList<>();
    for (Map<String, AttributeValue> item : items)
      writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());

    // scatter
    final int batch = 25;
    // List<CompletableFuture<BatchWriteItemResponse>> futures = new ArrayList<>();
    for (int fromIndex = 0; fromIndex < writeRequests.size(); fromIndex += batch) {
      int toIndex = fromIndex + batch;
      if (writeRequests.size() < toIndex)
        toIndex = writeRequests.size();

      List<WriteRequest> subList = writeRequests.subList(fromIndex, toIndex);

      BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
          //
          .requestItems(ImmutableMap.of(tableName, subList))
          //
          .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
          //
          .build();

      // log("batchWriteItem", fromIndex, toIndex);
      pool.execute(() -> {
        DynamoDbClient dynamo = DynamoDbClient.create();
        BatchWriteItemResponse batchWriteItemResponse = dynamo.batchWriteItem(batchWriteItemRequest);
        Double consumedCapacityUnits = batchWriteItemResponse.consumedCapacity().iterator().next().capacityUnits();
        wcuMeter.mark(consumedCapacityUnits.longValue());
        if (consumedCapacityUnits.intValue() > 0)
          wcuLimiter.acquire(consumedCapacityUnits.intValue());
        log(Double.valueOf(wcuMeter.getMeanRate()).intValue());
      });
    }

    // // gather
    // for (CompletableFuture<BatchWriteItemResponse> future : futures) {
    //   BatchWriteItemResponse batchWriteItemResponse = future.get();

    //     // String.format("%s/%s", Double.valueOf(rcuMeter().getMeanRate()).intValue(), Double.valueOf(wcuMeter().getMeanRate()).intValue()),

    // }

    MoreExecutors.shutdownAndAwaitTermination(pool, Duration.ofMillis(Long.MAX_VALUE));

    log("done", Double.valueOf(wcuMeter.getMeanRate()).intValue());

  }

  private Map<String, AttributeValue> createItem(int i) {
    Map<String, AttributeValue> item = new HashMap<>();
    String key = Hashing.sha256().hashInt(i).toString();
    item.put("key", AttributeValue.builder().s(key).build());
    return item;
  }
  
  // public Main() throws Exception {
  //   log("ctor");

  //   // start time
  //   final long t0 = System.currentTimeMillis();

  //   final BatchWriter topicWriter = new BatchWriter(false, 2000);
  //   try {
  //     log("start");
  //     topicWriter.start();

  //     final RateLimiter rateLimiter = RateLimiter.create(10000); // per second
  //     for (int i = 0; i < 100000; ++i) {
  //       JsonObject userRecord = new JsonObject();
  //       userRecord.addProperty("uuid", UUID.randomUUID().toString());
  //       userRecord.addProperty("foo", "bar");
  //       userRecord.addProperty("baz", new Random().nextInt());
  //       userRecord.addProperty("version", System.currentTimeMillis());

  //       // does not block
  //       topicWriter.addUserRecord(userRecord);

  //       // rate limit
  //       rateLimiter.acquire();
  //     }

  //   } finally {
  //     log("close");
  //     topicWriter.close();
  //     log("closed");
  //   }

  //   // finish time
  //   log(System.currentTimeMillis() - t0);

  // }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}