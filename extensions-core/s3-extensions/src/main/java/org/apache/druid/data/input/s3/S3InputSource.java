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

import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import org.apache.commons.io.FilenameUtils;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3InputSource extends CloudObjectInputSource
{
  // We lazily initialize ServerSideEncryptingAmazonS3 to avoid costly s3 operation when we only need S3InputSource
  // for stored information (such as for task logs) and not for ingestion.
  // (This cost only applies for new ServerSideEncryptingAmazonS3 created with s3InputSourceConfig given).
  private final Supplier<ServerSideEncryptingAmazonS3> s3ClientSupplier;
  @JsonProperty("properties")
  private final S3InputSourceConfig s3InputSourceConfig;
  private final S3InputDataConfig inputDataConfig;
  private final AWSProxyConfig awsProxyConfig;
  private final AWSClientConfig awsClientConfig;
  private final AWSEndpointConfig awsEndpointConfig;
  private int maxRetries;

  /**
   * Constructor for S3InputSource
   *
   * @param s3Client            The default ServerSideEncryptingAmazonS3 client built with all default configs
   *                            from Guice. This injected singleton client is use when {@param s3InputSourceConfig}
   *                            is not provided and hence, we can skip building a new client from
   *                            {@param s3ClientBuilder}
   * @param s3ClientBuilder     Use for building a new s3Client to use instead of the default injected
   *                            {@param s3Client}. The configurations of the client can be changed
   *                            before being built
   * @param inputDataConfig     Stores the configuration for options related to reading input data
   * @param uris                User provided uris to read input data
   * @param prefixes            User provided prefixes to read input data
   * @param objects             User provided cloud objects values to read input data
   * @param s3InputSourceConfig User provided properties for overriding the default S3 credentials
   * @param awsProxyConfig      User provided proxy information for the overridden s3 client
   * @param awsEndpointConfig   User provided s3 endpoint and region for overriding the default S3 endpoint
   * @param awsClientConfig     User provided properties for the S3 client with the overridden endpoint
   */
  @JsonCreator
  public S3InputSource(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JacksonInject ServerSideEncryptingAmazonS3.Builder s3ClientBuilder,
      @JacksonInject S3InputDataConfig inputDataConfig,
      @JacksonInject AWSCredentialsProvider awsCredentialsProvider,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("filter") @Nullable String filter,
      @JsonProperty("properties") @Nullable S3InputSourceConfig s3InputSourceConfig,
      @JsonProperty("proxyConfig") @Nullable AWSProxyConfig awsProxyConfig,
      @JsonProperty("endpointConfig") @Nullable AWSEndpointConfig awsEndpointConfig,
      @JsonProperty("clientConfig") @Nullable AWSClientConfig awsClientConfig
  )
  {
    super(S3StorageDruidModule.SCHEME, uris, prefixes, objects, filter);
    this.inputDataConfig = Preconditions.checkNotNull(inputDataConfig, "S3DataSegmentPusherConfig");
    Preconditions.checkNotNull(s3Client, "s3Client");
    this.s3InputSourceConfig = s3InputSourceConfig;
    this.awsProxyConfig = awsProxyConfig;
    this.awsClientConfig = awsClientConfig;
    this.awsEndpointConfig = awsEndpointConfig;

    this.s3ClientSupplier = Suppliers.memoize(
        () -> {
          if (s3ClientBuilder != null && s3InputSourceConfig != null) {
            if (awsEndpointConfig != null && awsEndpointConfig.getUrl() != null) {
              s3ClientBuilder
                  .getAmazonS3ClientBuilder().setEndpointConfiguration(
                      new AwsClientBuilder.EndpointConfiguration(
                          awsEndpointConfig.getUrl(),
                          awsEndpointConfig.getSigningRegion()
                      ));
              if (awsClientConfig != null) {
                s3ClientBuilder
                    .getAmazonS3ClientBuilder()
                    .withChunkedEncodingDisabled(awsClientConfig.isDisableChunkedEncoding())
                    .withPathStyleAccessEnabled(awsClientConfig.isEnablePathStyleAccess())
                    .withForceGlobalBucketAccessEnabled(awsClientConfig.isForceGlobalBucketAccessEnabled());

                if (awsProxyConfig != null) {
                  final Protocol protocol = S3Utils.determineProtocol(awsClientConfig, awsEndpointConfig);
                  s3ClientBuilder
                      .getAmazonS3ClientBuilder()
                      .withClientConfiguration(S3Utils.setProxyConfig(
                          s3ClientBuilder.getAmazonS3ClientBuilder()
                                         .getClientConfiguration(),
                          awsProxyConfig
                      ).withProtocol(protocol));
                }
              }
            }
            if (s3InputSourceConfig.isCredentialsConfigured()) {
              if (s3InputSourceConfig.getAssumeRoleArn() == null) {
                s3ClientBuilder
                    .getAmazonS3ClientBuilder()
                    .withCredentials(createStaticCredentialsProvider(s3InputSourceConfig));
              } else {
                applyAssumeRole(
                    s3ClientBuilder,
                    s3InputSourceConfig,
                    createStaticCredentialsProvider(s3InputSourceConfig)
                );
              }
            } else {
              applyAssumeRole(s3ClientBuilder, s3InputSourceConfig, awsCredentialsProvider);
            }
            return s3ClientBuilder.build();
          } else {
            return s3Client;
          }
        }
    );
    this.maxRetries = RetryUtils.DEFAULT_MAX_TRIES;
  }

  @VisibleForTesting
  S3InputSource(
      ServerSideEncryptingAmazonS3 s3Client,
      ServerSideEncryptingAmazonS3.Builder s3ClientBuilder,
      S3InputDataConfig inputDataConfig,
      List<URI> uris,
      List<URI> prefixes,
      List<CloudObjectLocation> objects,
      String filter,
      S3InputSourceConfig s3InputSourceConfig,
      AWSProxyConfig awsProxyConfig,
      AWSEndpointConfig awsEndpointConfig,
      AWSClientConfig awsClientConfig
  )
  {
    this(
        s3Client,
        s3ClientBuilder,
        inputDataConfig,
        null,
        uris,
        prefixes,
        objects,
        filter,
        s3InputSourceConfig,
        awsProxyConfig,
        awsEndpointConfig,
        awsClientConfig
    );
  }

  @VisibleForTesting
  public S3InputSource(
      ServerSideEncryptingAmazonS3 s3Client,
      ServerSideEncryptingAmazonS3.Builder s3ClientBuilder,
      S3InputDataConfig inputDataConfig,
      List<URI> uris,
      List<URI> prefixes,
      List<CloudObjectLocation> objects,
      String filter,
      S3InputSourceConfig s3InputSourceConfig,
      AWSProxyConfig awsProxyConfig,
      AWSEndpointConfig awsEndpointConfig,
      AWSClientConfig awsClientConfig,
      int maxRetries
  )
  {
    this(
        s3Client,
        s3ClientBuilder,
        inputDataConfig,
        null,
        uris,
        prefixes,
        objects,
        filter,
        s3InputSourceConfig,
        awsProxyConfig,
        awsEndpointConfig,
        awsClientConfig
    );
    this.maxRetries = maxRetries;
  }

  private void applyAssumeRole(
      ServerSideEncryptingAmazonS3.Builder s3ClientBuilder,
      S3InputSourceConfig s3InputSourceConfig,
      AWSCredentialsProvider awsCredentialsProvider
  )
  {
    String assumeRoleArn = s3InputSourceConfig.getAssumeRoleArn();
    if (assumeRoleArn != null) {
      String roleSessionName = StringUtils.format("druid-s3-input-source-%s", UUID.randomUUID().toString());
      AWSSecurityTokenService securityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
                                                                                         .withCredentials(
                                                                                             awsCredentialsProvider)
                                                                                         .build();
      STSAssumeRoleSessionCredentialsProvider.Builder roleCredentialsProviderBuilder;
      roleCredentialsProviderBuilder = new STSAssumeRoleSessionCredentialsProvider
          .Builder(assumeRoleArn, roleSessionName).withStsClient(securityTokenService);

      if (s3InputSourceConfig.getAssumeRoleExternalId() != null) {
        roleCredentialsProviderBuilder.withExternalId(s3InputSourceConfig.getAssumeRoleExternalId());
      }

      s3ClientBuilder.getAmazonS3ClientBuilder().withCredentials(roleCredentialsProviderBuilder.build());
    }
  }

  @Nonnull
  private AWSStaticCredentialsProvider createStaticCredentialsProvider(S3InputSourceConfig s3InputSourceConfig)
  {
    return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
            s3InputSourceConfig.getAccessKeyId().getPassword(),
            s3InputSourceConfig.getSecretAccessKey().getPassword()
        )
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

  @Override
  protected InputEntity createEntity(CloudObjectLocation location)
  {
    return new S3Entity(s3ClientSupplier.get(), location, maxRetries);
  }

  @Override
  protected Stream<InputSplit<List<CloudObjectLocation>>> getPrefixesSplitStream(@Nonnull SplitHintSpec splitHintSpec)
  {
    final Iterator<List<S3ObjectSummary>> splitIterator = splitHintSpec.split(
        getIterableObjectsFromPrefixes().iterator(),
        object -> new InputFileAttribute(object.getSize())
    );

    return Streams.sequentialStreamFrom(splitIterator)
                  .map(objects -> objects.stream()
                                         .map(S3Utils::summaryToCloudObjectLocation)
                                         .collect(Collectors.toList()))
                  .map(InputSplit::new);
  }

  @Override
  public SplittableInputSource<List<CloudObjectLocation>> withSplit(InputSplit<List<CloudObjectLocation>> split)
  {
    return new S3InputSource(
        s3ClientSupplier.get(),
        null,
        inputDataConfig,
        null,
        null,
        null,
        split.get(),
        getFilter(),
        getS3InputSourceConfig(),
        getAwsProxyConfig(),
        getAwsEndpointConfig(),
        getAwsClientConfig()
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), s3InputSourceConfig);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    S3InputSource that = (S3InputSource) o;
    return Objects.equals(s3InputSourceConfig, that.s3InputSourceConfig) &&
           Objects.equals(awsProxyConfig, that.awsProxyConfig) &&
           Objects.equals(awsClientConfig, that.awsClientConfig) &&
           Objects.equals(awsEndpointConfig, that.awsEndpointConfig);
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           ", filter=" + getFilter() +
           ", s3InputSourceConfig=" + getS3InputSourceConfig() +
           ", awsProxyConfig=" + getAwsProxyConfig() +
           ", awsEndpointConfig=" + getAwsEndpointConfig() +
           ", awsClientConfig=" + getAwsClientConfig() +
           '}';
  }

  private Iterable<S3ObjectSummary> getIterableObjectsFromPrefixes()
  {
    return () -> {
      Iterator<S3ObjectSummary> iterator = S3Utils.objectSummaryIterator(
          s3ClientSupplier.get(),
          getPrefixes(),
          inputDataConfig.getMaxListingLength(),
          maxRetries
      );

      // Skip files that didn't match filter.
      if (org.apache.commons.lang.StringUtils.isNotBlank(getFilter())) {
        iterator = Iterators.filter(
            iterator,
            object -> FilenameUtils.wildcardMatch(object.getKey(), getFilter())
        );
      }

      return iterator;
    };
  }
}
