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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;

/**
 *
 */
public class S3StorageDruidModule implements DruidModule
{
  public static final String SCHEME = "s3_zip";

  private static final Logger log = new Logger(S3StorageDruidModule.class);

  private static ClientConfiguration setProxyConfig(ClientConfiguration conf, AWSProxyConfig proxyConfig)
  {
    if (StringUtils.isNotEmpty(proxyConfig.getHost())) {
      conf.setProxyHost(proxyConfig.getHost());
    }
    if (proxyConfig.getPort() != -1) {
      conf.setProxyPort(proxyConfig.getPort());
    }
    if (StringUtils.isNotEmpty(proxyConfig.getUsername())) {
      conf.setProxyUsername(proxyConfig.getUsername());
    }
    if (StringUtils.isNotEmpty(proxyConfig.getPassword())) {
      conf.setProxyPassword(proxyConfig.getPassword());
    }
    return conf;
  }

  @Nullable
  private static Protocol parseProtocol(@Nullable String protocol)
  {
    if (protocol == null) {
      return null;
    }

    if (protocol.equalsIgnoreCase("http")) {
      return Protocol.HTTP;
    } else if (protocol.equalsIgnoreCase("https")) {
      return Protocol.HTTPS;
    } else {
      throw new IAE("Unknown protocol[%s]", protocol);
    }
  }

  private static Protocol determineProtocol(AWSClientConfig clientConfig, AWSEndpointConfig endpointConfig)
  {
    final Protocol protocolFromClientConfig = parseProtocol(clientConfig.getProtocol());
    final String endpointUrl = endpointConfig.getUrl();
    if (StringUtils.isNotEmpty(endpointUrl)) {
      //noinspection ConstantConditions
      final URI uri = URIs.parse(endpointUrl, protocolFromClientConfig.toString());
      final Protocol protocol = parseProtocol(uri.getScheme());
      if (protocol != null && (protocol != protocolFromClientConfig)) {
        log.warn("[%s] protocol will be used for endpoint [%s]", protocol, endpointUrl);
      }
      return protocol;
    } else {
      return protocolFromClientConfig;
    }
  }

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
             .addBinding("s3")
             .to(S3TimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding("s3n")
             .to(S3TimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(S3DataSegmentKiller.class).in(LazySingleton.class);
    Binders.dataSegmentMoverBinder(binder).addBinding(SCHEME).to(S3DataSegmentMover.class).in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder)
           .addBinding(SCHEME)
           .to(S3DataSegmentArchiver.class)
           .in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding("s3").to(S3DataSegmentPusher.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentPusherConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentArchiverConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3StorageConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.sse.kms", S3SSEKmsConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.sse.custom", S3SSECustomConfig.class);

    Binders.taskLogsBinder(binder).addBinding("s3").to(S3TaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", S3TaskLogsConfig.class);
    binder.bind(S3TaskLogs.class).in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public ServerSideEncryptingAmazonS3 getAmazonS3Client(
      AWSCredentialsProvider provider,
      AWSProxyConfig proxyConfig,
      AWSEndpointConfig endpointConfig,
      AWSClientConfig clientConfig,
      S3StorageConfig storageConfig
  )
  {
    final ClientConfiguration configuration = new ClientConfigurationFactory().getConfig();
    final Protocol protocol = determineProtocol(clientConfig, endpointConfig);
    final AmazonS3ClientBuilder builder = AmazonS3Client
        .builder()
        .withCredentials(provider)
        .withClientConfiguration(setProxyConfig(configuration, proxyConfig).withProtocol(protocol))
        .withChunkedEncodingDisabled(clientConfig.isDisableChunkedEncoding())
        .withPathStyleAccessEnabled(clientConfig.isEnablePathStyleAccess())
        .withForceGlobalBucketAccessEnabled(clientConfig.isForceGlobalBucketAccessEnabled());

    if (StringUtils.isNotEmpty(endpointConfig.getUrl())) {
      builder.setEndpointConfiguration(
          new EndpointConfiguration(endpointConfig.getUrl(), endpointConfig.getSigningRegion())
      );
    }

    return new ServerSideEncryptingAmazonS3(builder.build(), storageConfig.getServerSideEncryption());
  }
}
