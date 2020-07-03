package helpers;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonObject;

public class BatchWriterTest {
  
  static long parseRate(String... args) {
    Pattern p = Pattern.compile("([0-9]+)tps");
    for (String arg : args) {
        Matcher m = p.matcher(arg);
        if (m.matches())
            return Long.parseLong(m.group(1));
    }
    return 1;
}

static long parseDuration(String... args) {
    Pattern p = Pattern.compile("([0-9]+)s");
    for (String arg : args) {
        Matcher m = p.matcher(arg);
        if (m.matches())
            return Long.parseLong(m.group(1));
    }
    return 1;
}

public static void main(String... args) throws Exception {
    AtomicLong requests = new AtomicLong();
    AtomicLong responses = new AtomicLong();
    final long t0 = System.currentTimeMillis();
    try {
        final long rate = parseRate(args);
        System.out.println("rate="+rate);
        final long seconds = parseDuration(args);
        System.out.println("seconds="+seconds);
        final RateLimiter rateLimiter = RateLimiter.create(rate); // per second
        final BatchWriter topicWriter = new BatchWriter(false);
        topicWriter.start();
        try {
            for (long i = 0; i < seconds; ++i) {
                for (long j = 0; j < rate; ++j) {
                    rateLimiter.acquire();

                    JsonObject userRecord = new JsonObject();
                    String key = Hashing.sha256().hashLong(j).toString();

                    userRecord.addProperty("entityKey", key);
                    userRecord.addProperty("entityType", "/foo/bar/baz");
                    userRecord.addProperty("version", System.currentTimeMillis());
                    // userRecord.addProperty("deleted", true);

                    requests.incrementAndGet();
                    topicWriter.addToBatch(userRecord).addListener(()->{
                        responses.incrementAndGet();
                    }, MoreExecutors.directExecutor());
                    topicWriter.flush();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            topicWriter.close();
        }
    } finally {
        System.out.println(requests + " / " + responses);
        System.out.println((System.currentTimeMillis() - t0) + "ms");
    }
}

}