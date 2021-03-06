package helpers;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;

import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

    // class AwsNotification {
    // 	public String Type;
    // 	public String MessageId;
    // 	public String TopicArn;
    // 	public String Timestamp;
    // 	public String Message;

    // 	public String toString() {
    // 		return new Gson().toJson(this);
    // 	}
    // }

// At-most-once aws sqs message receiver
public class QueueReceiver {

  private final String queueUrl;
  private final SqsAsyncClient sqsClient = SqsAsyncClient.create();

  private boolean running;
  private Function<String, ListenableFuture<?>> listener;

  private final List<ListenableFuture<?>> futures = Lists.newCopyOnWriteArrayList();


  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build()); // for backoff

  /**
   * ctor
   * 
   * @param queueUrl
   */
  public QueueReceiver(String queueUrl) {
    log("ctor", queueUrl);
    this.queueUrl = queueUrl;
  }

  /**
   * setListener
   * 
   * @param listener
   */
  public void setListener(Function<String, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  /**
   * start
   */
  public void start() {
    log("start", queueUrl);
    running = true;
    doReceiveMessage(0);
  }

  /**
   * close
   */
  public void close() {
    log("close", queueUrl);
    running = false;
    // executorService.shutdownNow();
    // futures.forEach(f->f.cancel(true));
  }

  class MessageConsumedRecord {
    public final String queueUrl;
    public boolean success;
    public String failureMessage;
    public final String body;
    public MessageConsumedRecord(String queueUrl, String body) {
      this.queueUrl = queueUrl;
      this.body = body;
    }
    public String toString() {
      return SplunkHelper.toString(this);
    }
  }

  private void doReceiveMessage(int i) {
    if (running) {
      ListenableFuture<?> lf = new FutureRunner(){{
        run(() -> {
          ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
              //
              .queueUrl(queueUrl)
              //
              .waitTimeSeconds(20)
              //
              .build();
          return lf(sqsClient.receiveMessage(receiveMessageRequest));
        }, receiveMessageResponse->{
          if (receiveMessageResponse.hasMessages()) {
            for (Message message : receiveMessageResponse.messages()) {
              String body = message.body();
              run(() -> {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    //
                    .queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build();
                return lf(sqsClient.deleteMessage(deleteMessageRequest));
              }, deleteMessageResponse -> {
                MessageConsumedRecord record = new MessageConsumedRecord(queueUrl, body);
                run(()->{
                  return listener.apply(body);
                }, result->{
                  record.success=true;
                }, e->{ // listener
                  record.failureMessage = ""+e;
                }, ()->{
                  log(record);
                });
              }, e->{ // deleteMessage
                log(e);
              });
            }
          }
        }, e->{ // receiveMessage
          log(e);
          run(()->{
            // backoff
            return Futures.scheduleAsync(()->Futures.immediateVoidFuture(), Duration.ofSeconds(25), executorService);
          });
        });
      }}.get();

      futures.add(lf);
      lf.addListener(()->{
        futures.remove(lf);
        doReceiveMessage(i);
      }, MoreExecutors.directExecutor());
    }
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

  public static void main(String... args) throws Exception {
    String queueUrl = "https://sqs.us-east-1.amazonaws.com/343892718819/asdf";
    final QueueReceiver queueReceiver = new QueueReceiver(queueUrl);
    queueReceiver.setListener(body->{
      // System.out.println(body);
      if (new Random().nextInt(2)==0)
        return Futures.immediateFailedFuture(new Exception("Oof!"));
      return Futures.immediateVoidFuture();
    });
    queueReceiver.start();
    try {
      Thread.sleep(Long.MAX_VALUE);
    } finally {
      queueReceiver.close();
    }
    System.out.println("done");
  }

}