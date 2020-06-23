package dlcm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import helpers.LogHelper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoReaderTest {

  public static void main(String... args) throws Exception {
    new DynamoReaderTest();
  }

  public DynamoReaderTest() throws Exception {

    final long t0 = System.currentTimeMillis();

    final DynamoReader dynamoReader = new DynamoReader("DlcmStack-TableCD117FA1-10BX86V213J7Z");
    dynamoReader.start();

    AtomicLong counter = new AtomicLong();

    try {

      // Map<String, AttributeValue> key = ImmutableMap.of("key",
      //     AttributeValue.builder().s("00e3d448ba79ee31b68784fd3890233ccf82c88e118984e7e129b921e87d7172").build());
      
      final int seconds = 120;
      final RateLimiter rateLimiter = RateLimiter.create(10000);
      for (int i = 0; i < seconds*rateLimiter.getRate(); ++i) {
        rateLimiter.acquire();

        // if (i%rateLimiter.getRate()==0)
        //   dynamoReader.flush();

          Map<String, AttributeValue> key = createKey(i%Double.valueOf(rateLimiter.getRate()).intValue());
        ListenableFuture<Map<String, AttributeValue>> future = dynamoReader.getItem(key);
        future.addListener(()->{
          try {
            // System.out.println(future.get());
            counter.incrementAndGet();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }, MoreExecutors.directExecutor());
      }

      // dynamoReader.flush();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      System.out.println("close[1]");
      dynamoReader.close().get();
      System.out.println("close[2]");

      log(System.currentTimeMillis()-t0, "ms");

      log("counter", counter);

    }

  }

  private Map<String, AttributeValue> createKey(int i) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("key", s(Hashing.sha256().hashInt(i).toString()));
    return item;
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