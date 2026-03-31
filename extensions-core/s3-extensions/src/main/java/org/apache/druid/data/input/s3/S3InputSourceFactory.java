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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3InputSourceFactory implements InputSourceFactory
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final ServerSideEncryptingAmazonS3.Builder s3ClientBuilder;
  private final S3InputSourceConfig s3InputSourceConfig;
  private final S3InputDataConfig inputDataConfig;
  private final AwsCredentialsProvider awsCredentialsProvider;
  private final AWSProxyConfig awsProxyConfig;
  private final AWSClientConfig awsClientConfig;
  private final AWSEndpointConfig awsEndpointConfig;

  @JsonCreator
  public S3InputSourceFactory(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JacksonInject ServerSideEncryptingAmazonS3.Builder s3ClientBuilder,
      @JacksonInject S3InputDataConfig inputDataConfig,
      @JacksonInject AwsCredentialsProvider awsCredentialsProvider,
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
    return create(inputFilePaths, null);
  }

  @Override
  public SplittableInputSource create(List<String> inputFilePaths, @Nullable Map<String, String> vendedCredentials)
  {
    // Use vended credentials if provided, otherwise fall back to configured credentials
    S3InputSourceConfig effectiveConfig = buildConfigWithVendedCredentials(vendedCredentials);

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
        SystemFields.none(),
        effectiveConfig,
        awsProxyConfig,
        awsEndpointConfig,
        awsClientConfig
    );
  }

  /**
   * Builds an S3InputSourceConfig using vended credentials from the catalog.
   * Falls back to the configured s3InputSourceConfig if no credentials are vended.
   *
   * @param vendedCredentials credential map from Iceberg catalog, may be null
   * @return S3InputSourceConfig with vended credentials or the original config
   */
  @Nullable
  private S3InputSourceConfig buildConfigWithVendedCredentials(@Nullable Map<String, String> vendedCredentials)
  {
    if (vendedCredentials == null || vendedCredentials.isEmpty()) {
      return s3InputSourceConfig;
    }

    // Iceberg REST catalog credential keys (from REST spec)
    String accessKeyId = vendedCredentials.get("s3.access-key-id");
    String secretAccessKey = vendedCredentials.get("s3.secret-access-key");
    String sessionToken = vendedCredentials.get("s3.session-token");

    if (accessKeyId != null && secretAccessKey != null) {
      return new S3InputSourceConfig(
          new DefaultPasswordProvider(accessKeyId),
          new DefaultPasswordProvider(secretAccessKey),
          null, // assumeRoleArn - not needed with vended credentials
          null, // assumeRoleExternalId
          sessionToken != null ? new DefaultPasswordProvider(sessionToken) : null
      );
    }

    // No S3 credentials in the vended map, fall back to configured config
    return s3InputSourceConfig;
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
