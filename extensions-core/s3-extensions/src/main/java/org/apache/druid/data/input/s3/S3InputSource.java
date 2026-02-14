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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CloudObjectSplitWidget;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class S3InputSource extends CloudObjectInputSource
{
  public static final String TYPE_KEY = S3StorageDruidModule.SCHEME;
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
   * @param awsCredentialsProvider The default credentials provider
   * @param uris                User provided uris to read input data
   * @param prefixes            User provided prefixes to read input data
   * @param objects             User provided cloud objects values to read input data
   * @param objectGlob          User provided globbing rule to filter input data path
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
      @JacksonInject AwsCredentialsProvider awsCredentialsProvider,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("objectGlob") @Nullable String objectGlob,
      @JsonProperty(SYSTEM_FIELDS_PROPERTY) @Nullable SystemFields systemFields,
      @JsonProperty("properties") @Nullable S3InputSourceConfig s3InputSourceConfig,
      @JsonProperty("proxyConfig") @Nullable AWSProxyConfig awsProxyConfig,
      @JsonProperty("endpointConfig") @Nullable AWSEndpointConfig awsEndpointConfig,
      @JsonProperty("clientConfig") @Nullable AWSClientConfig awsClientConfig
  )
  {
    super(S3StorageDruidModule.SCHEME, uris, prefixes, objects, objectGlob, systemFields);
    this.inputDataConfig = Preconditions.checkNotNull(inputDataConfig, "S3DataSegmentPusherConfig");
    Preconditions.checkNotNull(s3Client, "s3Client");
    this.s3InputSourceConfig = s3InputSourceConfig;
    this.awsProxyConfig = awsProxyConfig;
    this.awsClientConfig = awsClientConfig;
    this.awsEndpointConfig = awsEndpointConfig;

    this.s3ClientSupplier = Suppliers.memoize(
        () -> {
          if (s3ClientBuilder != null && s3InputSourceConfig != null) {
            // Build a custom S3Client with the provided configuration
            S3ClientBuilder customBuilder = S3Client.builder();

            // Configure endpoint and region
            if (awsEndpointConfig != null && awsEndpointConfig.getUrl() != null) {
              String endpointUrl = awsEndpointConfig.getUrl();
              // Ensure endpoint URL has a scheme
              if (!endpointUrl.startsWith("http://") && !endpointUrl.startsWith("https://")) {
                boolean useHttps = S3Utils.useHttps(awsClientConfig, awsEndpointConfig);
                endpointUrl = S3Utils.ensureEndpointHasScheme(endpointUrl, useHttps);
              }
              customBuilder.endpointOverride(URI.create(endpointUrl));
              if (awsEndpointConfig.getSigningRegion() != null) {
                customBuilder.region(Region.of(awsEndpointConfig.getSigningRegion()));
              }
            }

            // Configure S3-specific settings
            if (awsClientConfig != null) {
              S3Configuration.Builder s3ConfigBuilder = S3Configuration.builder()
                  .pathStyleAccessEnabled(awsClientConfig.isEnablePathStyleAccess())
                  .chunkedEncodingEnabled(!awsClientConfig.isDisableChunkedEncoding());
              customBuilder.serviceConfiguration(s3ConfigBuilder.build());
            }

            // Configure HTTP client with proxy if needed
            ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
            if (awsProxyConfig != null && awsProxyConfig.getHost() != null) {
              ProxyConfiguration proxyConfig = S3Utils.buildProxyConfiguration(awsProxyConfig);
              if (proxyConfig != null) {
                httpClientBuilder.proxyConfiguration(proxyConfig);
              }
            }
            customBuilder.httpClientBuilder(httpClientBuilder);

            // Configure credentials
            AwsCredentialsProvider credentialsProvider;
            if (s3InputSourceConfig.isCredentialsConfigured()) {
              credentialsProvider = createStaticCredentialsProvider(s3InputSourceConfig);
            } else {
              credentialsProvider = awsCredentialsProvider;
            }

            // Apply assume role if configured
            if (s3InputSourceConfig.getAssumeRoleArn() != null) {
              credentialsProvider = createAssumeRoleCredentialsProvider(
                  s3InputSourceConfig,
                  credentialsProvider
              );
            }

            customBuilder.credentialsProvider(credentialsProvider);

            // Build and wrap in ServerSideEncryptingAmazonS3
            return s3ClientBuilder
                .setS3ClientSupplier(customBuilder::build)
                .build();
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
      String objectGlob,
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
        objectGlob,
        SystemFields.none(),
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
      String objectGlob,
      SystemFields systemFields,
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
        objectGlob,
        systemFields,
        s3InputSourceConfig,
        awsProxyConfig,
        awsEndpointConfig,
        awsClientConfig
    );
    this.maxRetries = maxRetries;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public Set<String> getTypes()
  {
    return Collections.singleton(TYPE_KEY);
  }

  private AwsCredentialsProvider createAssumeRoleCredentialsProvider(
      S3InputSourceConfig s3InputSourceConfig,
      AwsCredentialsProvider baseCredentialsProvider
  )
  {
    String assumeRoleArn = s3InputSourceConfig.getAssumeRoleArn();
    String roleSessionName = StringUtils.format("druid-s3-input-source-%s", UUID.randomUUID().toString());

    StsClientBuilder stsBuilder = StsClient.builder()
        .credentialsProvider(baseCredentialsProvider);

    // If we have endpoint config, use its region for STS too
    if (awsEndpointConfig != null && awsEndpointConfig.getSigningRegion() != null) {
      stsBuilder.region(Region.of(awsEndpointConfig.getSigningRegion()));
    }

    StsClient stsClient = stsBuilder.build();

    AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
        .roleArn(assumeRoleArn)
        .roleSessionName(roleSessionName)
        .durationSeconds(3600);

    if (s3InputSourceConfig.getAssumeRoleExternalId() != null) {
      assumeRoleRequestBuilder.externalId(s3InputSourceConfig.getAssumeRoleExternalId());
    }

    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClient)
        .refreshRequest(assumeRoleRequestBuilder.build())
        .asyncCredentialUpdateEnabled(true)
        .staleTime(Duration.ofMinutes(3))
        .build();
  }

  @Nonnull
  private StaticCredentialsProvider createStaticCredentialsProvider(S3InputSourceConfig s3InputSourceConfig)
  {
    if (s3InputSourceConfig.getSessionToken() != null) {
      AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
          s3InputSourceConfig.getAccessKeyId().getPassword(),
          s3InputSourceConfig.getSecretAccessKey().getPassword(),
          s3InputSourceConfig.getSessionToken().getPassword()
      );
      return StaticCredentialsProvider.create(sessionCredentials);
    } else {
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
              s3InputSourceConfig.getAccessKeyId().getPassword(),
              s3InputSourceConfig.getSecretAccessKey().getPassword()
          )
      );
    }
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
  protected CloudObjectSplitWidget getSplitWidget()
  {
    class SplitWidget implements CloudObjectSplitWidget
    {
      @Override
      public Iterator<LocationWithSize> getDescriptorIteratorForPrefixes(List<URI> prefixes)
      {
        return Iterators.transform(
            S3Utils.objectSummaryWithBucketIterator(
                s3ClientSupplier.get(),
                prefixes,
                inputDataConfig.getMaxListingLength(),
                maxRetries
            ),
            object -> new LocationWithSize(object.getBucket(), object.getKey(), object.getSize())
        );
      }

      @Override
      public long getObjectSize(CloudObjectLocation location)
      {
        final HeadObjectResponse objectMetadata = S3Utils.getSingleObjectMetadata(
            s3ClientSupplier.get(),
            location.getBucket(),
            location.getPath()
        );

        return objectMetadata.contentLength();
      }
    }

    return new SplitWidget();
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
        getObjectGlob(),
        systemFields,
        getS3InputSourceConfig(),
        getAwsProxyConfig(),
        getAwsEndpointConfig(),
        getAwsClientConfig()
    );
  }

  @Override
  public Object getSystemFieldValue(InputEntity entity, SystemField field)
  {
    final S3Entity s3Entity = (S3Entity) entity;

    switch (field) {
      case URI:
        return s3Entity.getUri().toString();
      case BUCKET:
        return s3Entity.getObject().getBucket();
      case PATH:
        return s3Entity.getObject().getPath();
      default:
        return null;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        s3InputSourceConfig,
        awsProxyConfig,
        awsClientConfig,
        awsEndpointConfig,
        maxRetries
    );
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
           Objects.equals(awsEndpointConfig, that.awsEndpointConfig) &&
           maxRetries == that.maxRetries;
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           ", objectGlob=" + getObjectGlob() +
           (systemFields.getFields().isEmpty() ? "" : ", systemFields=" + systemFields) +
           ", s3InputSourceConfig=" + getS3InputSourceConfig() +
           ", awsProxyConfig=" + getAwsProxyConfig() +
           ", awsEndpointConfig=" + getAwsEndpointConfig() +
           ", awsClientConfig=" + getAwsClientConfig() +
           '}';
  }
}
