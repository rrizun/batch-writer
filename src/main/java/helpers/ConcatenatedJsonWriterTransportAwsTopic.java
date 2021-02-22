package helpers;

import com.google.common.base.Defaults;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

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
    public ListenableFuture<Void> send(String message) {
        return new AbstractFuture<Void>() {
            {
                PublishRequest publishRequest = PublishRequest.builder()
                        //
                        .topicArn(topicArn).message(message).build();
                ListenableFuture<PublishResponse> lf = CompletableFuturesExtra.toListenableFuture(snsClient.publish(publishRequest));
                lf.addListener(() -> {
                    try {
                        lf.get();
                        // success!
                        set(Defaults.defaultValue(Void.class));
                    } catch (Exception e) {
                        setException(e);
                    }
                }, MoreExecutors.directExecutor());
            }
        };
    }

}
