package helpers;

import java.util.concurrent.*;

import com.google.common.util.concurrent.*;
import com.google.gson.*;
import com.spotify.futures.*;

import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

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
    int cores = Runtime.getRuntime().availableProcessors();
    final String queueUrl = "https://us-east-2.queue.amazonaws.com/743203956339/DlcmStack-InputEventQueueDB57F075-1PG9FW17QDZSN";
    // final ExecutorService executor = Executors.newCachedThreadPool();
    for (int core = 0; core < cores; ++core) {
      new Thread() {
        public void run() {

          try {

            final QueueReceiver queueReceiver = new QueueReceiver(queueUrl);
            try {
              queueReceiver.start();
              Thread.sleep(Long.MAX_VALUE);
            } finally {
              queueReceiver.close();
            }
          } catch (Exception e) {

          }

        }
      }.start();
    }

    // final QueueReceiver queueReceiver = new QueueReceiver("https://us-east-2.queue.amazonaws.com/743203956339/DlcmStack-InputEventQueueDB57F075-1PG9FW17QDZSN");
    // try {
    //   queueReceiver.start();
    //   Thread.sleep(Long.MAX_VALUE);
    // } finally {
    //   queueReceiver.close();
    // }
  }

  private final String queueUrl;

  private final SqsAsyncClient sqsClient = SqsAsyncClient.create();

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
    synchronized(lock) {
      running = true;
      doReceiveMessage(0);
    }
  }

  private int busy;
  private final Object lock = new Object();

  public void close() throws Exception {
    log("close");
    synchronized (lock) {
      // signal
      running = false;

      // wait
      while (busy > 0)
        lock.wait();

        // close
      sqsClient.close();
    }
  }

  // must be locked
  private void doReceiveMessage(int i) {
    trace("doReceiveMessage", running, i);
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

      ListenableFuture<ReceiveMessageResponse> listenableFuture = lf(sqsClient.receiveMessage(receiveMessageRequest));
      // stats(i);
      ++busy;
      listenableFuture.addListener(()->{
        synchronized (lock) {
          try {
            ReceiveMessageResponse receiveMessageResponse = listenableFuture.get();
  
            trace(abbrev(receiveMessageResponse.toString()));
  
            if (receiveMessageResponse.hasMessages()) {
              for (Message message : receiveMessageResponse.messages()) {
  
                AwsNotification notification = new Gson().fromJson(message.body(), AwsNotification.class);
                JsonArray jsonArray = new Gson().fromJson(notification.Message, JsonArray.class);
  
                trace("receiveMessage", abbrev(jsonArray.toString()));
  
                receiveMeter.mark(jsonArray.size());
                stats(i);
  
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
  
                ListenableFuture<DeleteMessageResponse> deleteMessageResponseFuture = lf(sqsClient.deleteMessage(deleteMessageRequest));
                ++busy;
                deleteMessageResponseFuture.addListener(()->{
                  synchronized (lock) {
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
                      lock.notifyAll();
                    }
                  }
                }, MoreExecutors.directExecutor());
              }
            }
        
          } catch (Exception e) {
            log(e); //###TODO SET A FUTURE HERE???
          } finally {
            --busy;
            lock.notifyAll();
            doReceiveMessage(i);
          }
        }
      }, MoreExecutors.directExecutor());
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