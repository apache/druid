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

package org.apache.druid.data.input.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class S3InputSourceFactory implements InputSourceFactory
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final ServerSideEncryptingAmazonS3.Builder s3ClientBuilder;
  private final S3InputSourceConfig s3InputSourceConfig;
  private final S3InputDataConfig inputDataConfig;
  private final AWSCredentialsProvider awsCredentialsProvider;
  private final AWSProxyConfig awsProxyConfig;
  private final AWSClientConfig awsClientConfig;
  private final AWSEndpointConfig awsEndpointConfig;

  @JsonCreator
  public S3InputSourceFactory(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JacksonInject ServerSideEncryptingAmazonS3.Builder s3ClientBuilder,
      @JacksonInject S3InputDataConfig inputDataConfig,
      @JacksonInject AWSCredentialsProvider awsCredentialsProvider,
      @JsonProperty("properties") @Nullable S3InputSourceConfig s3InputSourceConfig,
      @JsonProperty("proxyConfig") @Nullable AWSProxyConfig awsProxyConfig,
      @JsonProperty("endpointConfig") @Nullable AWSEndpointConfig awsEndpointConfig,
      @JsonProperty("clientConfig") @Nullable AWSClientConfig awsClientConfig
  )
  {
    this.s3Client = s3Client;
    this.s3ClientBuilder = s3ClientBuilder;
    this.inputDataConfig = inputDataConfig;
    this.awsCredentialsProvider = awsCredentialsProvider;
    this.s3InputSourceConfig = s3InputSourceConfig;
    this.awsProxyConfig = awsProxyConfig;
    this.awsEndpointConfig = awsEndpointConfig;
    this.awsClientConfig = awsClientConfig;
  }

  @Override
  public SplittableInputSource create(List<String> inputFilePaths)
  {
    return new S3InputSource(
        s3Client,
        s3ClientBuilder,
        inputDataConfig,
        awsCredentialsProvider,
        inputFilePaths.stream().map(chosenPath -> {
          try {
            return new URI(StringUtils.replace(chosenPath, "s3a://", "s3://"));
          }
          catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        }).collect(
            Collectors.toList()),
        null,
        null,
        null,
        s3InputSourceConfig,
        awsProxyConfig,
        awsEndpointConfig,
        awsClientConfig
    );
  }

  @Nullable
  @JsonProperty("properties")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public S3InputSourceConfig getS3InputSourceConfig()
  {
    return s3InputSourceConfig;
  }

  @Nullable
  @JsonProperty("proxyConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public AWSProxyConfig getAwsProxyConfig()
  {
    return awsProxyConfig;
  }

  @Nullable
  @JsonProperty("clientConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public AWSClientConfig getAwsClientConfig()
  {
    return awsClientConfig;
  }

  @Nullable
  @JsonProperty("endpointConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public AWSEndpointConfig getAwsEndpointConfig()
  {
    return awsEndpointConfig;
  }

}
