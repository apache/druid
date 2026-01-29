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

package org.apache.druid.testing.utils;

import com.google.common.collect.Iterables;
import org.apache.druid.testing.tools.ITRetryUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.AddTagsToStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;

import java.io.FileInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KinesisAdminClient implements StreamAdminClient
{
  private final KinesisClient kinesisClient;

  public KinesisAdminClient(String endpoint) throws Exception
  {
    String pathToConfigFile = System.getProperty("override.config.path");
    Properties prop = new Properties();
    prop.load(new FileInputStream(pathToConfigFile));

    StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
        AwsBasicCredentials.create(
            prop.getProperty("druid_kinesis_accessKey"),
            prop.getProperty("druid_kinesis_secretKey")
        )
    );

    KinesisClientBuilder builder = KinesisClient.builder()
        .credentialsProvider(credentials);

    final Region regionFromEndpoint = parseRegionFromEndpoint(endpoint);

    if (endpoint != null && !endpoint.isEmpty()) {
      final String endpointWithScheme = endpoint.contains("://") ? endpoint : "https://" + endpoint;
      builder.endpointOverride(URI.create(endpointWithScheme));
    }

    if (regionFromEndpoint != null) {
      builder.region(regionFromEndpoint);
    }

    kinesisClient = builder.build();
  }

  private static Region parseRegionFromEndpoint(String endpoint)
  {
    if (endpoint == null) {
      return null;
    }

    // Try to parse region using AWS SDK utility
    String host = endpoint.contains("://")
        ? URI.create(endpoint).getHost()
        : endpoint;
    if (host == null) {
      host = endpoint;
    }

    Optional<Region> region = AwsHostNameUtils.parseSigningRegion(host, "kinesis");
    if (region.isPresent()) {
      return region.get();
    }

    // For LocalStack or custom endpoints, default to US_EAST_1
    String lowerEndpoint = endpoint.toLowerCase(Locale.ENGLISH);
    if (lowerEndpoint.contains("localhost") || lowerEndpoint.contains("127.0.0.1")) {
      return Region.US_EAST_1;
    }
    return null;
  }

  @Override
  public void createStream(String streamName, int shardCount, Map<String, String> tags)
  {
    kinesisClient.createStream(CreateStreamRequest.builder()
        .streamName(streamName)
        .shardCount(shardCount)
        .build());

    if (tags != null && !tags.isEmpty()) {
      kinesisClient.addTagsToStream(AddTagsToStreamRequest.builder()
          .streamName(streamName)
          .tags(tags)
          .build());
    }
  }

  @Override
  public void deleteStream(String streamName)
  {
    kinesisClient.deleteStream(DeleteStreamRequest.builder()
        .streamName(streamName)
        .build());
  }

  /**
   * This method updates the shard count of {@param streamName} to have a final shard count of {@param newShardCount}
   * If {@param blocksUntilStarted} is set to true, then this method will blocks until the resharding
   * started (but not nessesary finished), otherwise, the method will returns right after issue the reshard command
   */
  @Override
  public void updatePartitionCount(String streamName, int newShardCount, boolean blocksUntilStarted)
  {
    int originalShardCount = getStreamPartitionCount(streamName);
    if (originalShardCount == newShardCount) {
      return;
    }
    kinesisClient.updateShardCount(UpdateShardCountRequest.builder()
        .streamName(streamName)
        .targetShardCount(newShardCount)
        .scalingType(ScalingType.UNIFORM_SCALING)
        .build());

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

  @Override
  public boolean isStreamActive(String streamName)
  {
    return verifyStreamStatus(streamName, StreamStatus.ACTIVE);
  }

  @Override
  public int getStreamPartitionCount(String streamName)
  {
    Set<String> shardIds = new HashSet<>();
    String exclusiveStartShardId = null;
    boolean hasMoreShards = true;

    while (hasMoreShards) {
      DescribeStreamRequest.Builder requestBuilder = DescribeStreamRequest.builder()
          .streamName(streamName);
      if (exclusiveStartShardId != null) {
        requestBuilder.exclusiveStartShardId(exclusiveStartShardId);
      }

      StreamDescription description = kinesisClient.describeStream(requestBuilder.build()).streamDescription();
      List<String> shardIdResult = description.shards()
                                              .stream()
                                              .map(Shard::shardId)
                                              .collect(Collectors.toList());
      shardIds.addAll(shardIdResult);
      hasMoreShards = description.hasMoreShards();
      if (hasMoreShards) {
        exclusiveStartShardId = Iterables.getLast(shardIdResult);
      }
    }
    return shardIds.size();
  }

  @Override
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
    return getStreamDescription(streamName).streamStatusAsString();
  }

  private StreamDescription getStreamDescription(String streamName)
  {
    DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(
        DescribeStreamRequest.builder()
            .streamName(streamName)
            .build()
    );
    return describeStreamResponse.streamDescription();
  }
}
