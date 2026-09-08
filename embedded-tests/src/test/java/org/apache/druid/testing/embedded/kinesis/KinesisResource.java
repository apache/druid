/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.kinesis;

import com.google.common.collect.Iterables;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.utils.ITRetryUtil;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resource to run an AWS Kinesis deployment backed by Localstack.
 * The terms {@code topic} and {@code stream} have been used interchangeably, as
 * have the terms {@code partition} and {@code shard}.
 */
public class KinesisResource extends StreamIngestResource<LocalStackContainer>
{
  private static final String IMAGE = "localstack/localstack:4.13.1";
  // Kinesis PutRecords accepts at most 500 records in a single request.
  private static final int PUT_RECORDS_BATCH_SIZE = 500;

  private KinesisClient kinesisClient;

  @Override
  protected LocalStackContainer createContainer()
  {
    return new LocalStackContainer(DockerImageName.parse(IMAGE))
        .withServices("kinesis");
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    kinesisClient = newClient();

    cluster
        .addExtensions(AWSModule.class, KinesisIndexingServiceModule.class)
        .addCommonProperty("druid.kinesis.accessKey", getContainer().getAccessKey())
        .addCommonProperty("druid.kinesis.secretKey", getContainer().getSecretKey());
  }

  public String getEndpoint()
  {
    return getContainer().getEndpoint().toString();
  }

  public String getRegion()
  {
    return getContainer().getRegion();
  }

  private StaticCredentialsProvider getCredentials()
  {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(getContainer().getAccessKey(), getContainer().getSecretKey())
    );
  }

  @Override
  public void createTopicWithPartitions(String topic, int partitionCount)
  {
    kinesisClient.createStream(
        CreateStreamRequest.builder()
                           .streamName(topic)
                           .shardCount(partitionCount)
                           .build()
    );

    // Wait until stream is ACTIVE or UPDATING
    final Set<StreamStatus> acceptedStates = Set.of(StreamStatus.ACTIVE, StreamStatus.UPDATING);
    ITRetryUtil.retryUntilTrue(
        () -> acceptedStates.contains(getStreamStatus(topic)),
        "Stream is in state " + acceptedStates
    );
  }

  @Override
  public void deleteTopic(String topic)
  {
    kinesisClient.deleteStream(
        DeleteStreamRequest.builder()
                           .streamName(topic)
                           .build()
    );
  }

  @Override
  public void increasePartitionsInTopic(String topic, int newPartitionCount)
  {
    final int originalShardCount = getStreamShardCount(topic);
    if (originalShardCount == newPartitionCount) {
      return;
    }

    kinesisClient.updateShardCount(
        UpdateShardCountRequest.builder()
                               .streamName(topic)
                               .targetShardCount(newPartitionCount)
                               .scalingType(ScalingType.UNIFORM_SCALING)
                               .build()
    );

    // Wait until the stream is active/updating and shard count is updated
    final Set<StreamStatus> acceptedStates = Set.of(StreamStatus.ACTIVE, StreamStatus.UPDATING);
    ITRetryUtil.retryUntilTrue(
        () -> acceptedStates.contains(getStreamStatus(topic))
              && getStreamShardCount(topic) > originalShardCount,
        "Stream has updated shard count"
    );
  }

  @Override
  public void publishRecordsToTopic(String topic, List<byte[]> records)
  {
    publishRecordsInBatches(topic, records, record -> DigestUtils.sha1Hex(record));
  }

  @Override
  public void publishRecordsToTopicWithoutTransaction(String topic, List<byte[]> records)
  {
    publishRecordsToTopic(topic, records);
  }

  @Override
  public void publishRecordsToTopic(String topic, List<byte[]> records, Map<String, Object> properties)
  {
    publishRecordsToTopic(topic, records);
  }

  public void publishRecordsToTopicPartition(String topic, String partitionKey, List<byte[]> records)
  {
    publishRecordsInBatches(topic, records, record -> partitionKey);
  }

  private void publishRecordsInBatches(
      String topic,
      List<byte[]> records,
      Function<byte[], String> partitionKeyFunction
  )
  {
    for (int start = 0; start < records.size(); start += PUT_RECORDS_BATCH_SIZE) {
      final List<PutRecordsRequestEntry> entries = records.subList(
          start,
          Math.min(start + PUT_RECORDS_BATCH_SIZE, records.size())
      ).stream().map(record -> PutRecordsRequestEntry.builder()
                                                       .partitionKey(partitionKeyFunction.apply(record))
                                                       .data(SdkBytes.fromByteArray(record))
                                                       .build())
                  .collect(Collectors.toList());
      final PutRecordsResponse response = kinesisClient.putRecords(
          PutRecordsRequest.builder()
                           .streamName(topic)
                           .records(entries)
                           .build()
      );
      if (response.failedRecordCount() > 0) {
        throw new IllegalStateException("Failed to publish " + response.failedRecordCount() + " Kinesis records");
      }
    }
  }

  private KinesisClient newClient()
  {
    return KinesisClient.builder()
                        .credentialsProvider(getCredentials())
                        .endpointOverride(URI.create(getEndpoint()))
                        .region(Region.of(getContainer().getRegion()))
                        .build();
  }

  private int getStreamShardCount(String topic)
  {
    Set<String> shardIds = new HashSet<>();
    DescribeStreamRequest.Builder requestBuilder = DescribeStreamRequest.builder()
                                                                        .streamName(topic);
    String exclusiveStartShardId = null;
    boolean hasMore = true;
    while (hasMore) {
      if (exclusiveStartShardId != null) {
        requestBuilder.exclusiveStartShardId(exclusiveStartShardId);
      }
      StreamDescription description = kinesisClient.describeStream(requestBuilder.build())
                                                   .streamDescription();
      List<String> shardIdResult = description.shards()
                                              .stream()
                                              .map(Shard::shardId)
                                              .collect(Collectors.toList());
      shardIds.addAll(shardIdResult);
      if (description.hasMoreShards()) {
        exclusiveStartShardId = Iterables.getLast(shardIdResult);
      } else {
        hasMore = false;
      }
    }
    return shardIds.size();
  }

  private StreamStatus getStreamStatus(String streamName)
  {
    return getStreamDescription(streamName).streamStatus();
  }

  private StreamDescription getStreamDescription(String streamName)
  {
    DescribeStreamResponse response = kinesisClient.describeStream(
        DescribeStreamRequest.builder()
                             .streamName(streamName)
                             .build()
    );
    return response.streamDescription();
  }
}
