package dlcm;

import java.util.HashMap;
import java.util.Map;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.RateLimiter;

import helpers.LogHelper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Main {

  public static void main(String... args) throws Exception {
    new Main().run();
  }

  private final String tableName = "DlcmStack-TableCD117FA1-GVAWLZWY3HBE";

  public void run() throws Exception {
    log("run");

        // start time
        final long t0 = System.currentTimeMillis();
        
        final DynamoWriter dynamoWriter = new DynamoWriter(tableName, 2000);
        try {
          log("start");
            dynamoWriter.start();
            log("started");

            final RateLimiter rateLimiter = RateLimiter.create(10000); // per second
            for (int i = 0; i < 100000; ++i) {
                // non-blocking
                 dynamoWriter.addWriteRequest(createItem(i%10000));

                 // rate limit
                rateLimiter.acquire();
            }

        } finally {
            log("close");
            dynamoWriter.close();
            log("closed");
        }
        
        // finish time
        log(System.currentTimeMillis() - t0, "ms");

        log("writeRequestCount", dynamoWriter.writeRequestCount);
        log("totalConsumedCapacity", dynamoWriter.total);

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