package helpers;

import static org.junit.jupiter.api.Assertions.*;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class ConcatenatedJsonWriterTest {

    final int MTU = 256 * 1024; // sns/sqs

    private MeterRegistry registry = new SimpleMeterRegistry();
    {
        Metrics.addRegistry(registry);
    }

    private final List<String> sendMessages = new ArrayList<>();

    private final ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriter.Transport() {

        @Override
        public int mtu() {
            return MTU;
        }

        @Override
        public ListenableFuture<?> send(String message) {
            // log("send", message);
            if (message.length() > mtu())
                fail();
            sendMessages.add(message);
            return Futures.immediateVoidFuture();
        }
    };
    
    private final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport, registry);

    @Test
    public void basicSmoke() throws Exception {

        Exception e = assertThrows(Exception.class, ()->{
            Futures.allAsList(writer.write(null), writer.flush()).get();
        });
        log("expected", e);
        //###TODO verify counters

        Futures.allAsList(write("{}"), flush()).get();
        assertEquals(stream("{}"), stream(sendMessages));
        //###TODO verify counters

        Futures.allAsList(write("{}"), flush()).get();
        assertEquals(stream("{}{}"), stream(sendMessages));
        //###TODO verify counters

        Futures.allAsList(write("{}"), write("{}"), flush()).get();
        assertEquals(stream("{}{}{}{}"), stream(sendMessages));
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
        assertThrows(Exception.class, () -> {
            Futures.allAsList(write(value), flush()).get();
        });
        assertEquals(stream(), stream(sendMessages));
        counts(0, 1);
    }

    @Test
    public void testRandom() throws Exception {
        int count = 0;
        final int batchCount = 1 + new SecureRandom().nextInt(50);
        for (int i = 0; i < batchCount; ++i) {
            final int perBatch = 1 + new SecureRandom().nextInt(50);
            for (int j = 0; j < perBatch; ++j, ++count)
                write(String.format("\"%s\"", random(transport.mtu() / 10)));
            flush().get();
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

    private void counts(long in, long inErr) {
        assertEquals(in, writer.in.count());
        assertEquals(inErr, writer.inErr.count());
    }

    private String random(int len) {
        byte[] bytes = new byte[new SecureRandom().nextInt(len)];
        new SecureRandom().nextBytes(bytes);
        return BaseEncoding.base64Url().encode(bytes);
    }

    // convenience
    private ListenableFuture<?> write(String json) { // not concatenatedJson
        return writer.write(new JsonStreamParser(json).next());
    }

    // convenience
    private ListenableFuture<?> flush() {
        return writer.flush();
    }
    
    // convenience
    private Iterable<JsonElement> stream(String... concatenatedJson) {
        return stream(Arrays.asList(concatenatedJson));
    }

    // convenience
    private Iterable<JsonElement> stream(Iterable<String> concatenatedJson) {
        List<JsonElement> stream = new ArrayList<>();
        for (String s : concatenatedJson) {
            Iterators.addAll(stream, new JsonStreamParser(s));
            // stream.addAll(Lists.newArrayList(new JsonStreamParser(s)));
        }
        return stream;
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}
