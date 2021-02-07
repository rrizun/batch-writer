import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.Defaults;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;

import org.junit.jupiter.api.Test;

import helpers.ConcatenatedJsonTopicPublisher;
import helpers.FutureRunner;

public class ConcatenatedJsonWriterTest {

    private String sentMessage;

    ConcatenatedJsonTopicPublisher.Transport transport = new ConcatenatedJsonTopicPublisher.Transport() {

        @Override
        public int mtu() {
            return 2;
        }

        @Override
        public ListenableFuture<Void> send(String message) {
            if (message.length()>mtu())
                return Futures.immediateFailedFuture(new Exception("mtu"));
            sentMessage = message;
            return Futures.immediateFuture(Defaults.defaultValue(Void.class));
        }
    };
    
    private final ConcatenatedJsonTopicPublisher writer = new ConcatenatedJsonTopicPublisher(transport);

    @Test
    public void test() throws Exception {
        ListenableFuture<Void> request = writer.request(new JsonObject());
        writer.publish().get();
        request.get();
        assertEquals("{}", sentMessage);
    }
}
