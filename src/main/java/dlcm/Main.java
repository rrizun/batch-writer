package dlcm;

import java.util.HashMap;
import java.util.Map;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;

import org.checkerframework.checker.nullness.qual.Nullable;

import helpers.LogHelper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Main {

  public static void main(String... args) throws Exception {
    new Main().run();
  }

  private final String tableName = "DlcmStack-TableCD117FA1-10BX86V213J7Z";

  int successCount;
  int failureCount;

  public void run() throws Exception {
    log("run");

    // start time
    final long t0 = System.currentTimeMillis();

    final DynamoWriter dynamoWriter = new DynamoWriter(tableName);
    try {
      log("start");
      dynamoWriter.start();
      log("started");

      final int rate = 128; // per second
      final int numSeconds = 3600;
      final RateLimiter rateLimiter = RateLimiter.create(rate);
      for (int i = 0; i < rate * numSeconds; ++i) {
        Futures.addCallback(dynamoWriter.addWriteRequest(createItem(i % 10000)), new FutureCallback<Void>() {
          @Override
          public void onSuccess(@Nullable Void result) {
            ++successCount;
          }
          @Override
          public void onFailure(Throwable t) {
            log(t);
            ++failureCount;
          }
        }, dynamoWriter.executor());

        // rate limit
        rateLimiter.acquire();
      }

      // squeeze out that last batch
      dynamoWriter.flush();

    } finally {
      log("close");
      dynamoWriter.close();
      log("closed");
    }

    // finish time
    log(System.currentTimeMillis() - t0, "ms");

    log("writeRequestCount", dynamoWriter.writeRequestCount);
    log("totalConsumedCapacity", dynamoWriter.total);

    log("successCount", successCount);
    log("failureCount", failureCount);


  }

  private Map<String, AttributeValue> createItem(int i) {
    Map<String, AttributeValue> item = new HashMap<>();
    String key = Hashing.sha256().hashInt(i).toString();
    item.put("key", AttributeValue.builder().s(key).build());
    return item;
  }
  
  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}