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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ScalingType;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.google.common.collect.Iterables;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.tools.ITRetryUtil;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resource to run an AWS Kinesis deployment backed by Localstack.
 * The terms {@code topic} and {@code stream} have been used interchangeably, as
 * have the terms {@code partition} and {@code shard}.
 */
public class KinesisResource extends StreamIngestResource<LocalStackContainer>
{
  private static final String IMAGE = "localstack/localstack:4.13.1";

  private AmazonKinesis amazonKinesis;

  @Override
  protected LocalStackContainer createContainer()
  {
    return new LocalStackContainer(DockerImageName.parse(IMAGE))
        .withServices("kinesis");
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    amazonKinesis = newClient();

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

  private AWSCredentialsProvider getCredentials()
  {
    return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(getContainer().getAccessKey(), getContainer().getSecretKey())
    );
  }

  @Override
  public void createTopicWithPartitions(String topic, int partitionCount)
  {
    CreateStreamResult createStreamResult = amazonKinesis.createStream(topic, partitionCount);
    if (createStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot create stream for integration test");
    }

    // Wait until stream is ACTIVE or UPDATING
    final Set<String> acceptedStates = Set.of(StreamStatus.ACTIVE.toString(), StreamStatus.UPDATING.toString());
    ITRetryUtil.retryUntilTrue(
        () -> acceptedStates.contains(getStreamStatus(topic)),
        "Stream is in state " + acceptedStates
    );
  }

  @Override
  public void deleteTopic(String topic)
  {
    DeleteStreamResult deleteStreamResult = amazonKinesis.deleteStream(topic);
    if (deleteStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot delete stream for integration test");
    }
  }

  @Override
  public void increasePartitionsInTopic(String topic, int newPartitionCount)
  {
    final int originalShardCount = getStreamShardCount(topic);
    if (originalShardCount == newPartitionCount) {
      return;
    }

    final UpdateShardCountRequest updateShardCountRequest = new UpdateShardCountRequest()
        .withStreamName(topic)
        .withTargetShardCount(newPartitionCount)
        .withScalingType(ScalingType.UNIFORM_SCALING);
    final UpdateShardCountResult updateShardCountResult = amazonKinesis.updateShardCount(updateShardCountRequest);
    if (updateShardCountResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot update stream's shard count for integration test");
    }

    // Wait until the stream is active/updating and shard count is updated
    final Set<String> acceptedStates = Set.of(StreamStatus.ACTIVE.toString(), StreamStatus.UPDATING.toString());
    ITRetryUtil.retryUntilTrue(
        () -> acceptedStates.contains(getStreamStatus(topic))
              && getStreamShardCount(topic) > originalShardCount,
        "Stream has updated shard count"
    );
  }

  @Override
  public void publishRecordsToTopic(String topic, List<byte[]> records)
  {
    for (byte[] record : records) {
      amazonKinesis.putRecord(
          topic,
          ByteBuffer.wrap(record),
          DigestUtils.sha1Hex(record)
      );
    }
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
    for (byte[] record : records) {
      final PutRecordRequest putRequest = new PutRecordRequest()
          .withStreamName(topic)
          .withPartitionKey(partitionKey)
          .withData(ByteBuffer.wrap(record));
      amazonKinesis.putRecord(putRequest);
    }
  }

  private AmazonKinesis newClient()
  {
    return AmazonKinesisClientBuilder
        .standard()
        .withCredentials(getCredentials())
        .withClientConfiguration(new ClientConfiguration())
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                getEndpoint(),
                getContainer().getRegion()
            )
        ).build();
  }

  private int getStreamShardCount(String topic)
  {
    Set<String> shardIds = new HashSet<>();
    DescribeStreamRequest request = new DescribeStreamRequest();
    request.setStreamName(topic);
    while (request != null) {
      StreamDescription description = amazonKinesis.describeStream(request).getStreamDescription();
      List<String> shardIdResult = description.getShards()
                                              .stream()
                                              .map(Shard::getShardId)
                                              .collect(Collectors.toList());
      shardIds.addAll(shardIdResult);
      if (description.isHasMoreShards()) {
        request.setExclusiveStartShardId(Iterables.getLast(shardIdResult));
      } else {
        request = null;
      }
    }
    return shardIds.size();
  }

  private String getStreamStatus(String streamName)
  {
    return StringUtils.toUpperCase(getStreamDescription(streamName).getStreamStatus());
  }

  private StreamDescription getStreamDescription(String streamName)
  {
    DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(streamName);
    if (describeStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot get stream description for integration test");
    }
    return describeStreamResult.getStreamDescription();
  }
}
