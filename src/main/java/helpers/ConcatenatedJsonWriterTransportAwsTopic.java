package helpers;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * ConcatenatedJsonWriterTransportAwsTopic
 */
public class ConcatenatedJsonWriterTransportAwsTopic implements ConcatenatedJsonWriter.Transport {
    private final SnsAsyncClient snsClient;
    private final String topicArn;

    /**
     * ctor
     * 
     * @param snsClient
     * @param topicArn
     */
    public ConcatenatedJsonWriterTransportAwsTopic(SnsAsyncClient snsClient, String topicArn) {
        this.snsClient = snsClient;
        this.topicArn = topicArn;
    }

    @Override
    public int mtu() {
        return 256 * 1024; // sns/sqs max msg len
    }

    @Override
    public ListenableFuture<?> send(String message) {
        PublishRequest publishRequest = PublishRequest.builder().topicArn(topicArn).message(message).build();
        return CompletableFuturesExtra.toListenableFuture(snsClient.publish(publishRequest));
    }

}
