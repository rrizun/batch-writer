package dlcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import helpers.LogHelper;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoWriter {

    // config
    private final String tableName;
    private final long lingerMs;

    private final DynamoDbClient dynamo = DynamoDbClient.builder()
    //
    .httpClientBuilder(ApacheHttpClient.builder()
    //
    .maxConnections(10000/25)) //###TODO  NEEDED???
    //
    .build();


    // batch thread
    private final ScheduledExecutorService batchThread = Executors.newSingleThreadScheduledExecutor();
    
    // batch thread state
    // private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // private JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(baos));
    private List<WriteRequest> requestItems = new ArrayList<>();
    // private ScheduledFuture<?> scheduledPublishFuture;


    // publish threads
    private final ExecutorService publishPool = Executors.newCachedThreadPool();

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter wcuMeter = metrics.meter("wcu");

    /**
     * ctor
     * 
     * @param tableName
     * @param lingerMs
     * @throws Exception
     */
    public DynamoWriter(String tableName, long lingerMs) throws Exception {
        this.tableName = tableName;
        this.lingerMs = lingerMs;
    }

    public void start() throws Exception {
        batchThread.execute(() -> {
            try {
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        schedulePublish();
    }

    public void close() throws Exception {
        batchThread.execute(()->{
            try {
                publishNow(); // does not block
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        MoreExecutors.shutdownAndAwaitTermination(batchThread, Duration.ofMillis(Long.MAX_VALUE));
        MoreExecutors.shutdownAndAwaitTermination(publishPool, Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * addUserRecord
     */
    public void addWriteRequest(Map<String, AttributeValue> item) {
        batchThread.execute(() -> {
            try {
                requestItems.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());
                if (requestItems.size() == 25)
                    publishNow();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void schedulePublish() {
        // scheduledPublishFuture = batchThread.schedule(()->{
        //     try {
        //         publishNow(); // does not block
        //     } catch (Exception e) {
        //         throw new RuntimeException(e);
        //     }
        // }, lingerMs, TimeUnit.MILLISECONDS);
    }

    private void cancelScheduledPublish() {
        // scheduledPublishFuture.cancel(true);
    }

    // this is run within the batchPool context
    private void publishNow() throws Exception {
        // cancel scheduled publish
        cancelScheduledPublish();

        if (requestItems.size() > 0) {

            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    //
                    .requestItems(ImmutableMap.of(tableName, requestItems))
                    //
                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    //
                    .build();

            publishPool.execute(() -> {
                try {
                    BatchWriteItemResponse batchWriteItemResponse = dynamo.batchWriteItem(batchWriteItemRequest);
                    Double consumedCapacityUnits = batchWriteItemResponse.consumedCapacity().iterator().next()
                            .capacityUnits();
                    wcuMeter.mark(consumedCapacityUnits.longValue());
                    log("wcuMeterMeanRate", Double.valueOf(wcuMeter.getMeanRate()).intValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            requestItems = new ArrayList<>();
        }

        // reschedule scheduled publish
        schedulePublish();
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }
 
}