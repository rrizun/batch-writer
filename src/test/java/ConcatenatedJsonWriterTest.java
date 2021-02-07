import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Defaults;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;

import org.junit.jupiter.api.Test;

import helpers.ConcatenatedJsonWriter;

public class ConcatenatedJsonWriterTest {

    private final ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriter.Transport() {

        @Override
        public int mtu() {
            return 2000;
        }

        @Override
        public String[] tags() {
            return new String[]{};
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

    private final List<String> sentMessages = new ArrayList<>();

    @Test
    public void test() throws Exception {
        ListenableFuture<Void> lf;
        
        lf = writer.request(new JsonObject());
        writer.send().get();
        // lf.get();
        assertEquals("{}\n", sentMessages.get(0));

        lf = writer.request(new JsonObject());
        writer.send().get();
        // lf.get();
        assertEquals("{}\n", sentMessages.get(1));
    }

    @Test
    public void testb() throws Exception {
        ListenableFuture<Void> lf1, lf2;
        
        lf1 = writer.request(new JsonObject());
        lf2 = writer.request(new JsonObject());
        writer.send().get();
        // lf1.get();
        // lf2.get();
        assertEquals("{}\n{}\n", sentMessages.get(0));
    }

    @Test
    public void testmtu() throws Exception {
        ListenableFuture<Void> lf1, lf2;
        
        lf1 = writer.request(new JsonObject());
        lf2 = writer.request(new JsonObject());
        writer.send().get();
        // lf1.get();
        // lf2.get();
        assertEquals("{}\n{}\n", sentMessages.get(0));
    }
}
