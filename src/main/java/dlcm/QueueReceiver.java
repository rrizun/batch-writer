package dlcm;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.spotify.futures.CompletableFuturesExtra;

import helpers.LogHelper;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class AwsNotification {
	public String Type;
	public String MessageId;
	public String TopicArn;
	public String Timestamp;
	public String Message;

	public String toString() {
		return new Gson().toJson(this);
	}
}

public class QueueReceiver {

  public static void main(String... args) throws Exception {
    new QueueReceiver("https://us-east-2.queue.amazonaws.com/743203956339/DlcmStack-InputEventQueueDB57F075-1PG9FW17QDZSN").start();
    Thread.sleep(Long.MAX_VALUE);
  }

  private final String queueUrl;

  private final Executor executor = MoreExecutors.directExecutor();
  private final SqsAsyncClient sqs = SqsAsyncClient.create();

  // private final MetricRegistry registry = new MetricRegistry();
  // private final Meter meter = registry.meter("asdf");
  private final MyMeter meter = new MyMeter(5);

  private int errorCount;

  /**
   * ctor
   * 
   * @throws Exception
   */
  public QueueReceiver(String queueUrl) throws Exception {
    log("ctor");
    this.queueUrl = queueUrl;
  }

  public void start() {
    for (int i = 0; i < 4; ++i)
      doReceiveMessage();

    //
    new Timer().scheduleAtFixedRate(new TimerTask(){
      @Override
      public void run() {
        log("averageReceiveRate/s", meter.average(), "errorCount", errorCount);
      }
    }, 0, 2000);
  }

  public void close() {
    //###TODO
  }

  private void doReceiveMessage() {
    trace("doReceiveMessage");
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
        //
        .queueUrl(queueUrl)
        // The maximum long polling wait time is 20 seconds
        .waitTimeSeconds(20)
        //
        .build();

    trace(receiveMessageRequest);

    ListenableFuture<ReceiveMessageResponse> listenableFuture = lf(sqs.receiveMessage(receiveMessageRequest));
    listenableFuture.addListener(()->{
      try {
        ReceiveMessageResponse receiveMessageResponse = listenableFuture.get();

        trace("responseMetadata", receiveMessageResponse.responseMetadata());

        if (receiveMessageResponse.hasMessages()) {
          for (Message message : receiveMessageResponse.messages()) {
            trace(message.body().length());

            AwsNotification notification = new Gson().fromJson(message.body(), AwsNotification.class);
            JsonArray array = new Gson().fromJson(notification.Message, JsonArray.class);
            meter.mark(array.size());
    
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                //
                .queueUrl(queueUrl)
                //
                .receiptHandle(message.receiptHandle())
                //
                .build();
    
    
            // DeleteMessageResponse deleteMessageResponse;
            
            trace(deleteMessageRequest);
            lf(sqs.deleteMessage(deleteMessageRequest)).addListener(()->{
              trace("deleteMessage.listener");
              // log(deleteMessageResponse);
            }, executor);
    
          }
        }
    
      } catch (Exception e) {
        ++errorCount;
        log(e);
        // e.printStackTrace();
      } finally {
        doReceiveMessage();
      }
    }, executor);

  }

  private <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
    return CompletableFuturesExtra.toListenableFuture(cf);
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

  private void trace(Object... args) {
    // new LogHelper(this).log(args);
  }

}