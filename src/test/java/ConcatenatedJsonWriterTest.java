import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Defaults;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.junit.jupiter.api.Test;

import helpers.ConcatenatedJsonWriter;

public class ConcatenatedJsonWriterTest {

    private final List<String> sentMessages = new ArrayList<>();

    ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriter.Transport() {

        @Override
        public int mtu() {
            return 6;
        }

        @Override
        public ListenableFuture<Void> send(String message) {
            if (message.length()>mtu())
                return Futures.immediateFailedFuture(new Exception("mtu"));
            sentMessages.add(message);
            return Futures.immediateFuture(Defaults.defaultValue(Void.class));
        }
    };
    
    private final ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport);

    @Test
    public void test() throws Exception {
        writer.request(new JsonPrimitive("1"));
        writer.send().get();
        assertEquals("\"1\"", sentMessages.get(0));

        writer.request(new JsonPrimitive("2"));
        writer.send().get();
        assertEquals("\"2\"", sentMessages.get(1));
    }
}
