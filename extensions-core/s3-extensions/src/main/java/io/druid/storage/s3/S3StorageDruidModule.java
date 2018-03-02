/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import io.druid.common.aws.AWSCredentialsConfig;
import io.druid.common.aws.AWSEndpointConfig;
import io.druid.common.aws.AWSProxyConfig;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 */
public class S3StorageDruidModule implements DruidModule
{
  public static final String SCHEME = "s3_zip";

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
    JsonConfigProvider.bind(binder, "druid.s3", AWSCredentialsConfig.class);
    JsonConfigProvider.bind(binder, "druid.s3.proxy", AWSProxyConfig.class);
    JsonConfigProvider.bind(binder, "druid.s3.endpoint", AWSEndpointConfig.class);
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding("s3")
             .to(S3TimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding("s3n")
             .to(S3TimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    Binders.dataSegmentPullerBinder(binder).addBinding(SCHEME).to(S3DataSegmentPuller.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(S3DataSegmentKiller.class).in(LazySingleton.class);
    Binders.dataSegmentMoverBinder(binder).addBinding(SCHEME).to(S3DataSegmentMover.class).in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder)
           .addBinding(SCHEME)
           .to(S3DataSegmentArchiver.class)
           .in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding("s3").to(S3DataSegmentPusher.class).in(LazySingleton.class);
    Binders.dataSegmentFinderBinder(binder).addBinding("s3").to(S3DataSegmentFinder.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentPusherConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentArchiverConfig.class);

    Binders.taskLogsBinder(binder).addBinding("s3").to(S3TaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", S3TaskLogsConfig.class);
    binder.bind(S3TaskLogs.class).in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public AmazonS3 getAmazonS3Client(
      AWSCredentialsProvider provider,
      AWSProxyConfig proxyConfig,
      AWSEndpointConfig endpointConfig
  )
  {
    final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withCredentials(provider);
    final ClientConfiguration configuration = new ClientConfiguration();

    builder.withClientConfiguration(setProxyConfig(configuration, proxyConfig));
    if (toEndpointConfiguration(endpointConfig) != null) {
      builder.withEndpointConfiguration(toEndpointConfiguration(endpointConfig));
    }

    log.info(builder.getEndpoint().getServiceEndpoint());
    log.info(builder.getEndpoint().getSigningRegion());
    log.info(builder.getClientConfiguration().getProxyHost());
    log.info(builder.getClientConfiguration().getProxyPassword());
    return builder.build();
  }

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

  private static EndpointConfiguration toEndpointConfiguration(AWSEndpointConfig endpointConfig)
  {
    if (StringUtils.isNotEmpty(endpointConfig.getUrl())) {
      return new EndpointConfiguration(endpointConfig.getUrl(), endpointConfig.getSigningRegion());
    } else {
      return null;
    }
  }
}
