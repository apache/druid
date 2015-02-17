/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.druid.common.aws.AWSCredentialsConfig;
import io.druid.common.aws.AWSCredentialsUtils;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

import java.util.List;

/**
 */
public class S3StorageDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.s3", AWSCredentialsConfig.class);

    Binders.dataSegmentPullerBinder(binder).addBinding("s3_zip").to(S3DataSegmentPuller.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding("s3_zip").to(S3DataSegmentKiller.class).in(LazySingleton.class);
    Binders.dataSegmentMoverBinder(binder).addBinding("s3_zip").to(S3DataSegmentMover.class).in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder).addBinding("s3_zip").to(S3DataSegmentArchiver.class).in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding("s3").to(S3DataSegmentPusher.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentPusherConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentArchiverConfig.class);

    Binders.taskLogsBinder(binder).addBinding("s3").to(S3TaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", S3TaskLogsConfig.class);
    binder.bind(S3TaskLogs.class).in(LazySingleton.class);
    binder.bind(AWSCredentialsGuiceBridge.class).toProvider(AWSCredentialsGuiceBridgeProvider.class).asEagerSingleton(); // Fix lack of DI in ServiceLoader
  }

  private static class AWSCredentialsGuiceBridgeProvider implements Provider<AWSCredentialsGuiceBridge>
  {
    private final AWSCredentialsConfig config;
    @Inject
    public AWSCredentialsGuiceBridgeProvider(final AWSCredentialsConfig config){
      this.config = config;
    }
    public AWSCredentialsGuiceBridge get()
    {
      AWSCredentialsGuiceBridge bridge = new AWSCredentialsGuiceBridge();
      bridge.setCredentials(AWSCredentialsUtils.defaultAWSCredentialsProviderChain(config));
      return bridge;
    }
  }

  @Provides
  @LazySingleton
  public AWSCredentialsProvider getAWSCredentialsProvider(final AWSCredentialsConfig config)
  {
    return AWSCredentialsUtils.defaultAWSCredentialsProviderChain(config);
  }

  @Provides
  @LazySingleton
  public RestS3Service getRestS3Service(AWSCredentialsProvider provider)
  {
    if(provider.getCredentials() instanceof com.amazonaws.auth.AWSSessionCredentials) {
      return new RestS3Service(new AWSSessionCredentialsAdapter(provider));
    } else {
      return new RestS3Service(new AWSCredentials(
          provider.getCredentials().getAWSAccessKeyId(),
          provider.getCredentials().getAWSSecretKey()
      ));
    }
  }
}
