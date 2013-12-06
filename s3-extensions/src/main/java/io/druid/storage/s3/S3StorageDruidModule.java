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

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import org.jets3t.service.S3ServiceException;
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
    Binders.dataSegmentPusherBinder(binder).addBinding("s3").to(S3DataSegmentPusher.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentPusherConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage", S3DataSegmentMoverConfig.class);

    Binders.taskLogsBinder(binder).addBinding("s3").to(S3TaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", S3TaskLogsConfig.class);
    binder.bind(S3TaskLogs.class).in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public AWSCredentials getJets3tAWSCredentials(AWSCredentialsConfig config)
  {
    return new AWSCredentials(config.getAccessKey(), config.getSecretKey());
  }

  @Provides
  @LazySingleton
  public RestS3Service getRestS3Service(AWSCredentials credentials)
  {
    try {
      return new RestS3Service(credentials);
    }
    catch (S3ServiceException e) {
      throw new ProvisionException("Unable to create a RestS3Service", e);
    }
  }
}
