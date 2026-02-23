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
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.waiters.WaiterParameters;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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
