package helpers;

import com.google.common.util.concurrent.MoreExecutors;

import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class AwsSdkTwo {

  static final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder()
  //
  // .maxConcurrency(50*Runtime.getRuntime().availableProcessors())
  // //
  // .maxPendingConnectionAcquires(10_000)
  //
  ;

  static final ClientAsyncConfiguration clientAsyncConfiguration = ClientAsyncConfiguration.builder()
          //
          .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, MoreExecutors.directExecutor())
          //
          .build();

  static final DynamoDbAsyncClient dynamo = DynamoDbAsyncClient.builder()
          //
          .httpClientBuilder(httpClientBuilder)
          //
          .asyncConfiguration(clientAsyncConfiguration)
          //
          .build();

}