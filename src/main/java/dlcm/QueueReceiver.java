package dlcm;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.spotify.futures.CompletableFuturesExtra;

import helpers.LogHelper;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
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
    final QueueReceiver queueReceiver = new QueueReceiver("https://us-east-2.queue.amazonaws.com/743203956339/DlcmStack-InputEventQueueDB57F075-1PG9FW17QDZSN");
    try {
      queueReceiver.start();
      Thread.sleep(Long.MAX_VALUE);
    } finally {
      queueReceiver.close();
    }
  }

  private final String queueUrl;

  private final SqsAsyncClient sqs = SqsAsyncClient.create();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  private final long periodSeconds = 5;
  private final MyMeter receiveMeter = new MyMeter();
  private final MyMeter successMeter = new MyMeter();
  private final MyMeter failureMeter = new MyMeter();

  private boolean running;

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
    log("start");
    running = true;
    executor.execute(()->{
      for (int i = 0; i < 16; ++i)
        doReceiveMessage(i);
    });
  }

  private int busy;
  private final Object busyCond = new Object();

  public void close() throws Exception {
    log("close");

    // signal
    running = false;

    // wait
    synchronized(busyCond) {
      while (busy>0)
        busyCond.wait();
    }

    // close
    if (MoreExecutors.shutdownAndAwaitTermination(executor, Duration.ofMillis(Long.MAX_VALUE)))
      sqs.close();
  }

  private void doReceiveMessage(int i) {
    trace("doReceiveMessage", i);
    if (running) {

      // ----------------------------------------------------------------------
      // receiveMessage
      // ----------------------------------------------------------------------

      ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
      //
      .queueUrl(queueUrl)
      // The maximum long polling wait time is 20 seconds
      .waitTimeSeconds(20)
      //
      .build();

      trace(receiveMessageRequest);

      ListenableFuture<ReceiveMessageResponse> listenableFuture = lf(sqs.receiveMessage(receiveMessageRequest));
      stats(i);
      ++busy;
      listenableFuture.addListener(()->{
        try {
          ReceiveMessageResponse receiveMessageResponse = listenableFuture.get();

          trace(abbrev(receiveMessageResponse.toString()));

          if (receiveMessageResponse.hasMessages()) {
            for (Message message : receiveMessageResponse.messages()) {

              AwsNotification notification = new Gson().fromJson(message.body(), AwsNotification.class);
              JsonArray jsonArray = new Gson().fromJson(notification.Message, JsonArray.class);

              trace("receiveMessage", abbrev(jsonArray.toString()));

              receiveMeter.mark(jsonArray.size());

              // ----------------------------------------------------------------------
              // deleteMessage
              // ----------------------------------------------------------------------
      
              DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                  //
                  .queueUrl(queueUrl)
                  //
                  .receiptHandle(message.receiptHandle())
                  //
                  .build();

              trace(deleteMessageRequest);

              ListenableFuture<DeleteMessageResponse> deleteMessageResponseFuture = lf(sqs.deleteMessage(deleteMessageRequest));
              ++busy;
              deleteMessageResponseFuture.addListener(()->{
                try {
                  DeleteMessageResponse deleteMessageResponse = deleteMessageResponseFuture.get();
                  trace(deleteMessageResponse);
                  successMeter.mark(jsonArray.size());
                  stats(i);
                } catch (Exception e) {
                  log(e);
                  failureMeter.mark(jsonArray.size());
                  stats(i);
                } finally {
                  --busy;
                  synchronized (busyCond) {
                    busyCond.notifyAll();
                  }
                }
              }, executor);
            }
          }
      
        } catch (Exception e) {
          log(e);
          // e.printStackTrace();
        } finally {
          --busy;
          synchronized (busyCond) {
            busyCond.notifyAll();
          }
          if (running) {
            doReceiveMessage(i);
          }
        }
      }, executor);
    }
  }

  private void stats(int i) {
    log("receive", receiveMeter, "success", successMeter, "failure", failureMeter);
  }

  private <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
    return CompletableFuturesExtra.toListenableFuture(cf);
  }

  private String abbrev(String s) {
    if (s.length()>1024)
      s = s.substring(0,1024)+"...";
    return s;
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

  private void trace(Object... args) {
    // new LogHelper(this).log(args);
  }

}