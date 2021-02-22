package helpers;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class ConcatenatedJsonWriterTest {

    private final List<String> sendMessages = new ArrayList<>();

    private final ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriter.Transport() {

        @Override
        public int mtu() {
            return 2000;
        }

        @Override
        public ListenableFuture<Void> send(String message) {
            if (message.length() > mtu())
                throw new RuntimeException(String.format("message.len=%s mtu=%s", message.length(), mtu()));
            sendMessages.add(message);
            return Futures.immediateVoidFuture();
        }
    };
    
    private final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport, new String[]{});

    @Test
    public void test() throws Exception {
        Futures.allAsList(writer.write(new JsonObject()), writer.flush()).get();
        assertEquals(stream("{}"), stream(sendMessages));

        Futures.allAsList(writer.write(new JsonObject()), writer.flush()).get();
        assertEquals(stream("{}{}"), stream(sendMessages));

    }

    @Test
    public void testb() throws Exception {
        Futures.allAsList(writer.write(json("{}")), writer.write(json("{}")), writer.flush()).get();
        assertEquals(stream("{}{}"), stream(sendMessages));
    }

    @Test
    public void testCanSendLessThanMtu() throws Exception {
        String value = Strings.repeat("a", transport.mtu() / 2);
        Futures.allAsList(writer.write(new JsonPrimitive(value)), writer.flush()).get();
        assertEquals(stream(value), stream(sendMessages));
    }

    @Test
    public void testCantSendMoreThanMtu() throws Exception {
        String value = Strings.repeat("a", transport.mtu() + 1);
        assertThrows(Exception.class, () -> {
            Futures.allAsList(writer.write(new JsonPrimitive(value)), writer.flush()).get();
        });
        assertEquals(stream(), stream(sendMessages));
    }

    @Test
    public void testRandom() throws Exception {
        int count = 0;
        final int batchCount = random(500);
        for (int i = 0; i < batchCount; ++i) {
            final int perBatch = random(500);
            for (int j = 0; j < perBatch; ++j)
                writer.write(json(UUID.randomUUID().toString()));
            writer.flush().get();
            count += perBatch;
        }
        assertEquals(count, stream(sendMessages).size());
    }

    private int random(int bound) {
        return 1 + new Random().nextInt(bound);
    }

    // convenience
    private JsonElement json(String json) {
        return new Gson().fromJson(json, JsonElement.class);
    }

    // convenience
    private List<JsonElement> stream() {
        return ImmutableList.of();
    }

    // convenience
    private List<JsonElement> stream(String concatenatedJson) {
        return Lists.newArrayList(new JsonStreamParser(concatenatedJson));
    }

    // convenience
    private List<JsonElement> stream(List<String> concatenatedJsonList) {
        List<JsonElement> stream = new ArrayList<>();
        for (String concatenatedJson : concatenatedJsonList)
            stream.addAll(Lists.newArrayList(new JsonStreamParser(concatenatedJson)));
        return stream;
    }

    static {
        Metrics.addRegistry(new SimpleMeterRegistry());
    }
}
