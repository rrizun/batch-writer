package helpers;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.checkerframework.checker.nullness.qual.*;

import com.google.common.hash.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

class MyMeta {
  public String entityKey;
  public String entityType;
  public long version;
  public MyMeta(String entityKey, String entityType, long version) {
    this.entityKey= entityKey;
    this.entityType = entityType;
    this.version = version;
  }
}

public class DynamoWriterTest {

  public static void main(String... args) throws Exception {
    long rate = args.length > 0 ? Long.parseLong(args[0]) : 128;
    new DynamoWriterTest().run(rate);
  }

  private final String tableName = "DlcmStack-TableCD117FA1-10BX86V213J7Z";

  AtomicLong successCount = new AtomicLong();
  AtomicLong failureCount = new AtomicLong();

  public void run(long rate) throws Exception {
    log("run", rate);

    // start time
    final long t0 = System.currentTimeMillis();

    final DynamoWriter dynamoWriter = new DynamoWriter(tableName, DynamoDbAsyncClient.create());
    try {
      log("start");
      dynamoWriter.start();
      log("started");

      final RateLimiter rateLimiter = RateLimiter.create(rate);
      for (int i = 0; i < 25*rateLimiter.getRate(); ++i) {

        // rate limit
        rateLimiter.acquire();

        Futures.addCallback(dynamoWriter.addPutItem(createItem(i % 10000)), new FutureCallback<Void>() {
          @Override
          public void onSuccess(@Nullable Void result) {
            successCount.incrementAndGet();
          }
          @Override
          public void onFailure(Throwable t) {
            // log(t);
            failureCount.incrementAndGet();
          }
        }, MoreExecutors.directExecutor());

      }

      // dynamoWriter.flush().get();
      log("flush[1]");
      dynamoWriter.flush().get();
      log("flush[2]");

    } finally {
      log("close[1]");
      // AwsSdkTwo.dynamo.close();
      log("close[2]");

    }

    // finish time
    log(System.currentTimeMillis() - t0, "ms");

    log("successCount", successCount);
    log("failureCount", failureCount);

  }

  private Map<String, AttributeValue> createItem(int i) {
    String key = Hashing.sha256().hashInt(i).toString();
    MyMeta metaData = new MyMeta(key, "/foo/bar/baz", System.currentTimeMillis());

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("key", s(key));
    item.put("value", s(new Gson().toJson(metaData)));
    
    return item;
  }

  private AttributeValue s(String s) {
    return AttributeValue.builder().s(s).build();
  }
  
  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}