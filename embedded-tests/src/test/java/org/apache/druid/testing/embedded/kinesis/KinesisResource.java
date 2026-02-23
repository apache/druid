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
import com.amazonaws.services.kinesis.model.ScalingType;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.amazonaws.waiters.WaiterParameters;
import com.google.common.collect.Iterables;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.tools.ITRetryUtil;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        .addCommonProperty("druid.s3.accessKey", getContainer().getAccessKey())
        .addCommonProperty("druid.s3.secretKey", getContainer().getSecretKey());
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
  public void createStreamWithPartitions(String stream, int partitionCount)
  {
    CreateStreamResult createStreamResult = amazonKinesis.createStream(stream, partitionCount);
    if (createStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot create stream for integration test");
    }

    // Use SDK waiter (AWS SDK v1)
    amazonKinesis.waiters().streamExists().run(
        new WaiterParameters<>(new DescribeStreamRequest().withStreamName(stream))
    );
  }

  @Override
  public void publishRecordsToStream(String stream, List<byte[]> records)
  {
    for (byte[] record : records) {
      amazonKinesis.putRecord(
          stream,
          ByteBuffer.wrap(record),
          DigestUtils.sha1Hex(record)
      );
    }
  }

  @Override
  public void publishRecordsToStream(String stream, List<byte[]> records, Map<String, Object> properties)
  {
    publishRecordsToStream(stream, records);
  }

  public void deleteStream(String streamName)
  {
    DeleteStreamResult deleteStreamResult = amazonKinesis.deleteStream(streamName);
    if (deleteStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot delete stream for integration test");
    }
  }

  /**
   * Updates the shard count of {@param streamName} to have a final shard count of {@param newShardCount}
   * If {@param blocksUntilStarted} is set to true, then this method blocks until the resharding has
   * started (but not nessesary finished), otherwise, the method returns right after issue the reshard command.
   */
  public void updatePartitionCount(String streamName, int newShardCount, boolean blocksUntilStarted)
  {
    int originalShardCount = getStreamPartitionCount(streamName);
    if (originalShardCount == newShardCount) {
      return;
    }
    UpdateShardCountRequest updateShardCountRequest = new UpdateShardCountRequest();
    updateShardCountRequest.setStreamName(streamName);
    updateShardCountRequest.setTargetShardCount(newShardCount);
    updateShardCountRequest.setScalingType(ScalingType.UNIFORM_SCALING);
    UpdateShardCountResult updateShardCountResult = amazonKinesis.updateShardCount(updateShardCountRequest);
    if (updateShardCountResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot update stream's shard count for integration test");
    }
    if (blocksUntilStarted) {
      // Wait until the resharding started (or finished)
      ITRetryUtil.retryUntil(
          () -> {
            int updatedShardCount = getStreamPartitionCount(streamName);

            // Retry until Kinesis records the operation is either in progress (UPDATING) or completed (ACTIVE)
            // and the shard count has changed.

            return verifyStreamStatus(streamName, StreamStatus.ACTIVE, StreamStatus.UPDATING)
                   && updatedShardCount != originalShardCount;
          }, true,
          300, // higher value to avoid exceeding kinesis TPS limit
          100,
          "Kinesis stream resharding to start (or finished)"
      );
    }
  }

  public boolean isStreamActive(String streamName)
  {
    return verifyStreamStatus(streamName, StreamStatus.ACTIVE);
  }

  public int getStreamPartitionCount(String streamName)
  {
    Set<String> shardIds = new HashSet<>();
    DescribeStreamRequest request = new DescribeStreamRequest();
    request.setStreamName(streamName);
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

  public boolean verfiyPartitionCountUpdated(String streamName, int oldShardCount, int newShardCount)
  {
    int actualShardCount = getStreamPartitionCount(streamName);
    // Kinesis does not immediately drop the old shards after the resharding and hence,
    // would still returns both open shards and closed shards from the API call.
    // To verify, we sum the old count (closed shareds) and the expected new count (open shards)
    return actualShardCount == oldShardCount + newShardCount;
  }

  private boolean verifyStreamStatus(String streamName, StreamStatus... streamStatuses)
  {
    return Arrays.stream(streamStatuses)
                 .map(StreamStatus::toString)
                 .anyMatch(getStreamStatus(streamName)::equals);
  }

  private String getStreamStatus(String streamName)
  {
    return getStreamDescription(streamName).getStreamStatus();
  }

  private StreamDescription getStreamDescription(String streamName)
  {
    DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(streamName);
    if (describeStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot get stream description for integration test");
    }
    return describeStreamResult.getStreamDescription();
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
}
