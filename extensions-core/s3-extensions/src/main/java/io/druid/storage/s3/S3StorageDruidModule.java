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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import io.druid.common.aws.AWSCredentialsConfig;
import io.druid.data.SearchableVersionedDataFinder;
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
  public static final String SCHEME = "s3_zip";

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
  public RestS3Service getRestS3Service(AWSCredentialsProvider provider)
  {
    if (provider.getCredentials() instanceof AWSSessionCredentials) {
      return new RestS3Service(new AWSSessionCredentialsAdapter(provider));
    } else {
      return new RestS3Service(new AWSCredentials(
          provider.getCredentials().getAWSAccessKeyId(),
          provider.getCredentials().getAWSSecretKey()
      ));
    }
  }
}
