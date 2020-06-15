package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import helpers.LogHelper;

public class BatchWriter {

    // config
    private final boolean compress; // true to use gzip compression
    private final long lingerMs;

    // batch thread
    private final ScheduledExecutorService batchThread = Executors.newSingleThreadScheduledExecutor();
    
    // batch thread state
    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    private ScheduledFuture<?> scheduledPublishFuture;

    // publish threads
    private final ExecutorService publishPool = Executors.newCachedThreadPool();

    /**
     * ctor
     * 
     * @param compress
     * @param lingerMs
     * @throws Exception
     */
    public BatchWriter(boolean compress, long lingerMs) throws Exception {
        this.compress = compress;
        this.lingerMs = lingerMs;
    }

    public void start() throws Exception {
        batchThread.execute(() -> {
            try {
                jsonWriter.beginArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        schedulePublish();
    }

    public void close() throws Exception {
        batchThread.execute(()->{
            try {
                jsonWriter.endArray();
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // batchPool.shutdown();
        // batchPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE));

        // publishPool.shutdown();
        // publishPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        MoreExecutors.shutdownAndAwaitTermination(publishPool, Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * addUserRecord
     */
    public void addUserRecord(JsonElement jsonElement) {
        batchThread.execute(() -> {
            try {
                String jsonValue = jsonElement.toString();
                
                // will this exceed the max msg len?
                final int MAX_MSG_LEN = 256*1024; // sns/sqs 256KB
                if (baos.size() + jsonValue.length() > MAX_MSG_LEN - 1) { // the -1 is for closing the json array with a ']'
                    
                    // yes- publish now
                    jsonWriter.endArray();
                    publishNow(); // does not block
                    jsonWriter.beginArray();
                    
                }

                jsonWriter.jsonValue(jsonValue);
                jsonWriter.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void schedulePublish() {
        scheduledPublishFuture = batchThread.schedule(()->{
            try {
                jsonWriter.endArray();
                publishNow(); // does not block
                jsonWriter.beginArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, lingerMs, TimeUnit.MILLISECONDS);
    }

    private void cancelScheduledPublish() {
        scheduledPublishFuture.cancel(true);
    }

    // this is run within the batchPool context
    private void publishNow() throws Exception {

            // cancel scheduled publish
            cancelScheduledPublish();

            // STEP 1 close
            jsonWriter.close();

            // STEP 2 publish
            String[] utf8 = new String[] { new String(baos.toByteArray()) };
            if (compress) {
                ByteArrayOutputStream tmp = new ByteArrayOutputStream();
                try (OutputStream out = new GZIPOutputStream(tmp)) {
                    ByteStreams.copy(new ByteArrayInputStream(baos.toByteArray()), out);
                }
                utf8[0] = BaseEncoding.base64().encode(tmp.toByteArray());
            }
            // log(baos.size(), utf8[0].length());
            publishPool.execute(()->{
                log(utf8[0].length(), utf8[0].substring(0, Math.min(utf8[0].length(), 120)));
                //###TODO do actual sns publish here
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            
            // STEP 3 start
            baos.reset();
            jsonWriter = new JsonWriter(new OutputStreamWriter(baos));

            // reschedule scheduled publish
            schedulePublish();

    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }
 
    public static void main(String... args) throws Exception {
        
        // start time
        final long t0 = System.currentTimeMillis();
        
        final BatchWriter topicWriter = new BatchWriter(false, 2000);
        try {
            System.out.println("start");
            topicWriter.start();

            final RateLimiter rateLimiter = RateLimiter.create(1000000000); // per second
            for (int i = 0; i < 100000; ++i) {
                JsonObject userRecord = new JsonObject();
                userRecord.addProperty("foo", "bar");
                userRecord.addProperty("baz", new Random().nextInt());
                // userRecord.addProperty("uuid", UUID.randomUUID().toString());
                userRecord.addProperty("version", System.currentTimeMillis());

                // does not block
                 topicWriter.addUserRecord(userRecord);

                // rate limit
                rateLimiter.acquire();
            }

        } finally {
            System.out.println("close");
            topicWriter.close();
            System.out.println("closed");
        }
        
        // finish time
        System.out.println(System.currentTimeMillis() - t0);

    }


}