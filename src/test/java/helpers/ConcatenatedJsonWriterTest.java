package helpers;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
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
        public ListenableFuture<?> send(String message) {
            // log("send", message);
            if (message.length() > mtu())
                throw new RuntimeException(String.format("message.len=%s mtu=%s", message.length(), mtu()));
            sendMessages.add(message);
            return Futures.immediateVoidFuture();
        }
    };
    
    private final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport, new String[]{});

    @Test
    public void test() throws Exception {
        Futures.allAsList(write("{}"), flush()).get();
        assertEquals(stream("{}"), stream(sendMessages));
        //###TODO verify counters

        Futures.allAsList(write("{}"), flush()).get();
        assertEquals(stream("{}{}"), stream(sendMessages));
        //###TODO verify counters

    }

    @Test
    public void testb() throws Exception {
        Futures.allAsList(write("{}"), write("{}"), flush()).get();
        assertEquals(stream("{}{}"), stream(sendMessages));
        //###TODO verify counters
    }

    @Test
    public void testCanSendLessThanMtu() throws Exception {
        String value = Strings.repeat("a", transport.mtu() / 2);
        Futures.allAsList(write(value), flush()).get();
        assertEquals(stream(value), stream(sendMessages));
        //###TODO verify counters
    }

    @Test
    public void testCantSendMoreThanMtu() throws Exception {
        String value = Strings.repeat("a", transport.mtu() + 1);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            Futures.allAsList(write(value), flush()).get();
        });
        log("assertThrows.expected", e);
        assertEquals(stream(), stream(sendMessages));
        //###TODO verify counters
    }

    @Test
    public void testRandom() throws Exception {
        int count = 0;
        final int batchCount = random(500);
        for (int i = 0; i < batchCount; ++i) {
            final int perBatch = random(500);
            for (int j = 0; j < perBatch; ++j)
                write(UUID.randomUUID().toString());
            flush().get();
            count += perBatch;
        }
        assertEquals(count, Iterables.size(stream(sendMessages)));
        //###TODO verify counters
    }

    @Test
    public void testFlush() throws Exception {
        flush().get();
        assertEquals(stream(), stream(sendMessages));
        //###TODO verify counters

        Futures.allAsList(write("{}"), flush(), flush()).get();
        assertEquals(stream("{}"), stream(sendMessages));
        //###TODO verify counters

        Futures.allAsList(write("{}"), flush(), flush()).get();
        assertEquals(stream("{}{}"), stream(sendMessages));
        //###TODO verify counters
    }

    private int random(int bound) {
        return 1 + new Random().nextInt(bound);
    }

    // convenience
    private ListenableFuture<Void> write(String json) {
        return writer.write(new Gson().fromJson(json, JsonElement.class));
    }

    // convenience
    private ListenableFuture<Void> flush() {
        return writer.flush();
    }
    
    // convenience
    private Iterable<JsonElement> stream(String... concatenatedJson) {
        return stream(Arrays.asList(concatenatedJson));
    }

    // convenience
    private Iterable<JsonElement> stream(Iterable<String> concatenatedJson) {
        List<JsonElement> stream = new ArrayList<>();
        for (String s : concatenatedJson)
            stream.addAll(Lists.newArrayList(new JsonStreamParser(s)));
        return stream;
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

    static {
        Metrics.addRegistry(new SimpleMeterRegistry());
    }
}
