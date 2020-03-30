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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ScalingType;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.amazonaws.util.AwsHostNameUtils;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.IntegrationTestingConfig;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

public class KinesisAdminClient
{
  private AmazonKinesis amazonKinesis;

  @Inject
  public KinesisAdminClient(IntegrationTestingConfig config) throws Exception
  {
    String endpoint = config.getStreamEndpoint();
    String pathToConfigFile = System.getProperty("override.config.path");
    Properties prop = new Properties();
    prop.load(new FileInputStream(pathToConfigFile));

    AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
            prop.getProperty("druid_kinesis_accessKey"),
            prop.getProperty("druid_kinesis_secretKey")
        )
    );
    amazonKinesis = AmazonKinesisClientBuilder.standard()
                              .withCredentials(credentials)
                              .withClientConfiguration(new ClientConfiguration())
                              .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                                  endpoint,
                                  AwsHostNameUtils.parseRegion(
                                      endpoint,
                                      null
                                  )
                              )).build();
  }

  public void createStream(String streamName, int shardCount, Map<String, String> tags)
  {
    CreateStreamResult createStreamResult = amazonKinesis.createStream(streamName, shardCount);
    if (createStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot create stream for integration test");
    }
    if (tags != null && !tags.isEmpty()) {
      AddTagsToStreamRequest addTagsToStreamRequest = new AddTagsToStreamRequest();
      addTagsToStreamRequest.setStreamName(streamName);
      addTagsToStreamRequest.setTags(tags);
      AddTagsToStreamResult addTagsToStreamResult = amazonKinesis.addTagsToStream(addTagsToStreamRequest);
      if (addTagsToStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
        throw new ISE("Cannot tag stream for integration test");
      }
    }

  }

  public void deleteStream(String streamName)
  {
    DeleteStreamResult deleteStreamResult = amazonKinesis.deleteStream(streamName);
    if (deleteStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot delete stream for integration test");
    }
  }

  public void updateShardCount(String streamName, int newShardCount)
  {
    UpdateShardCountRequest updateShardCountRequest = new UpdateShardCountRequest();
    updateShardCountRequest.setStreamName(streamName);
    updateShardCountRequest.setTargetShardCount(newShardCount);
    updateShardCountRequest.setScalingType(ScalingType.UNIFORM_SCALING);
    UpdateShardCountResult updateShardCountResult = amazonKinesis.updateShardCount(updateShardCountRequest);
    if (updateShardCountResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot update stream's shard count for integration test");
    }
  }

  public boolean isStreamActive(String streamName)
  {
    DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(streamName);
    if (describeStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot get stream status for integration test");
    }
    return StreamStatus.ACTIVE.toString().equals(describeStreamResult.getStreamDescription().getStreamStatus());
  }

  public int getStreamShardCount(String streamName)
  {
    DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(streamName);
    if (describeStreamResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
      throw new ISE("Cannot get stream status for integration test");
    }
    return describeStreamResult.getStreamDescription().getShards().size();
  }
}
