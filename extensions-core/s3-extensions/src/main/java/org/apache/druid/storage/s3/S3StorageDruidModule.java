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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import javax.annotation.Nullable;
import java.net.URI;
import java.time.Duration;
import java.util.List;

/**
 *
 */
public class S3StorageDruidModule implements DruidModule
{
  public static final String SCHEME = "s3";
  public static final String SCHEME_S3N = "s3n";
  public static final String SCHEME_S3_ZIP = "s3_zip";

  private static final Logger log = new Logger(S3StorageDruidModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidS3-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(S3LoadSpec.class);
          }
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME)
             .to(S3TimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME_S3N)
             .to(S3TimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder)
           .addBinding(SCHEME_S3_ZIP)
           .to(S3DataSegmentKiller.class)
           .in(LazySingleton.class);
    Binders.dataSegmentMoverBinder(binder)
           .addBinding(SCHEME_S3_ZIP)
           .to(S3DataSegmentMover.class)
           .in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder)
           .addBinding(SCHEME_S3_ZIP)
           .to(S3DataSegmentArchiver.class)
           .in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(S3DataSegmentPusher.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3InputDataConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentPusherConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentArchiverConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3StorageConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.sse.kms", S3SSEKmsConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.sse.custom", S3SSECustomConfig.class);

    Binders.bindTaskLogs(binder, SCHEME, S3TaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", S3TaskLogsConfig.class);
  }

  // This provides ServerSideEncryptingAmazonS3.Builder with default configs from Guice injection initially set.
  // However, this builder can then be modified and have configuration(s) inside
  // S3ClientBuilder and/or S3StorageConfig overridden before being built.
  @Provides
  public ServerSideEncryptingAmazonS3.Builder getServerSideEncryptingAmazonS3Builder(
      AwsCredentialsProvider provider,
      AWSProxyConfig proxyConfig,
      AWSEndpointConfig endpointConfig,
      AWSClientConfig clientConfig,
      S3StorageConfig storageConfig
  )
  {
    final boolean useHttps = S3Utils.useHttps(clientConfig, endpointConfig);
    final URI endpointOverride = buildEndpointOverride(endpointConfig, useHttps);
    final Region region = StringUtils.isNotEmpty(endpointConfig.getSigningRegion())
        ? Region.of(endpointConfig.getSigningRegion())
        : null;

    final Supplier<S3Client> s3ClientSupplier = () -> {
      // Build HTTP client with proxy configuration
      ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
          .connectionTimeout(Duration.ofMillis(clientConfig.getConnectionTimeout()))
          .socketTimeout(Duration.ofMillis(clientConfig.getSocketTimeout()))
          .maxConnections(clientConfig.getMaxConnections());

      ProxyConfiguration proxyConfiguration = S3Utils.buildProxyConfiguration(proxyConfig);
      if (proxyConfiguration != null) {
        httpClientBuilder.proxyConfiguration(proxyConfiguration);
      }

      // Build S3 configuration
      // Note: forcePathStyle is configured on the S3ClientBuilder, not in S3Configuration
      S3Configuration s3Configuration = S3Configuration.builder()
          .chunkedEncodingEnabled(!clientConfig.isDisableChunkedEncoding())
          .build();

      S3ClientBuilder s3ClientBuilder = S3Client.builder()
          .credentialsProvider(provider)
          .httpClientBuilder(httpClientBuilder)
          .serviceConfiguration(s3Configuration)
          .forcePathStyle(clientConfig.isEnablePathStyleAccess());

      if (endpointOverride != null) {
        s3ClientBuilder.endpointOverride(endpointOverride);
      }

      if (region != null) {
        s3ClientBuilder.region(region);
      }

      return s3ClientBuilder.build();
    };

    // Create async client supplier for S3TransferManager
    final Supplier<S3AsyncClient> s3AsyncClientSupplier = () -> {
      NettyNioAsyncHttpClient.Builder asyncHttpClientBuilder = NettyNioAsyncHttpClient.builder()
          .connectionTimeout(Duration.ofMillis(clientConfig.getConnectionTimeout()))
          .readTimeout(Duration.ofMillis(clientConfig.getSocketTimeout()))
          .maxConcurrency(clientConfig.getMaxConnections());

      S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder()
          .credentialsProvider(provider)
          .httpClientBuilder(asyncHttpClientBuilder)
          .forcePathStyle(clientConfig.isEnablePathStyleAccess())
          .multipartEnabled(true);

      if (endpointOverride != null) {
        s3AsyncClientBuilder.endpointOverride(endpointOverride);
      }

      if (region != null) {
        s3AsyncClientBuilder.region(region);
      }

      return s3AsyncClientBuilder.build();
    };

    return ServerSideEncryptingAmazonS3.builder()
                                       .setS3ClientSupplier(s3ClientSupplier)
                                       .setS3AsyncClientSupplier(s3AsyncClientSupplier)
                                       .setS3StorageConfig(storageConfig);
  }

  @Nullable
  private static URI buildEndpointOverride(AWSEndpointConfig endpointConfig, boolean useHttps)
  {
    if (StringUtils.isNotEmpty(endpointConfig.getUrl())) {
      return URI.create(S3Utils.ensureEndpointHasScheme(endpointConfig.getUrl(), useHttps));
    }
    return null;
  }

  // This provides ServerSideEncryptingAmazonS3 built with all default configs from Guice injection
  /**
   * Creates {@link ServerSideEncryptingAmazonS3} which may perform config validation immediately.
   * You may want to avoid immediate config validation but defer it until you actually use the s3 client.
   * Use {@link #getAmazonS3ClientSupplier} instead in that case.
   */
  @Provides
  @LazySingleton
  public ServerSideEncryptingAmazonS3 getAmazonS3Client(
      ServerSideEncryptingAmazonS3.Builder serverSideEncryptingAmazonS3Builder
  )
  {
    return serverSideEncryptingAmazonS3Builder.build();
  }

  /**
   * Creates a supplier that lazily initialize {@link ServerSideEncryptingAmazonS3}.
   * You may want to use the supplier to defer config validation until you actually use the s3 client.
   */
  @Provides
  @LazySingleton
  public Supplier<ServerSideEncryptingAmazonS3> getAmazonS3ClientSupplier(
      ServerSideEncryptingAmazonS3.Builder serverSideEncryptingAmazonS3Builder
  )
  {
    return Suppliers.memoize(serverSideEncryptingAmazonS3Builder::build);
  }
}
