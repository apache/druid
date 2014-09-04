/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
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
  }

  @Provides
  @LazySingleton
  public AWSCredentialsProvider getAWSCredentialsProvider(final AWSCredentialsConfig config)
  {
    if (!Strings.isNullOrEmpty(config.getAccessKey()) && !Strings.isNullOrEmpty(config.getSecretKey())) {
      return new AWSCredentialsProvider() {
        @Override
        public com.amazonaws.auth.AWSCredentials getCredentials() {
          return new com.amazonaws.auth.AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {
              return config.getAccessKey();
            }

            @Override
            public String getAWSSecretKey() {
              return config.getSecretKey();
            }
          };
        }

        @Override
        public void refresh() {}
      };
    } else {
      return new FileSessionCredentialsProvider(config.getFileSessionCredentials());
    }
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
