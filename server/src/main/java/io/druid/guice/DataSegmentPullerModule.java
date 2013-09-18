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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.druid.segment.loading.HdfsDataSegmentPuller;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.loading.OmniSegmentLoader;
import io.druid.segment.loading.S3DataSegmentPuller;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.cassandra.CassandraDataSegmentConfig;
import io.druid.segment.loading.cassandra.CassandraDataSegmentPuller;
import org.apache.hadoop.conf.Configuration;

/**
 */
public class DataSegmentPullerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(SegmentLoader.class).to(OmniSegmentLoader.class).in(LazySingleton.class);

    bindDeepStorageLocal(binder);
    bindDeepStorageS3(binder);
    bindDeepStorageHdfs(binder);
    bindDeepStorageCassandra(binder);
  }

  private static void bindDeepStorageLocal(Binder binder)
  {
    DruidBinders.dataSegmentPullerBinder(binder)
                .addBinding("local")
                .to(LocalDataSegmentPuller.class)
                .in(LazySingleton.class);
  }

  private static void bindDeepStorageS3(Binder binder)
  {
    DruidBinders.dataSegmentPullerBinder(binder)
                .addBinding("s3_zip")
                .to(S3DataSegmentPuller.class)
                .in(LazySingleton.class);
  }

  private static void bindDeepStorageHdfs(Binder binder)
  {
    DruidBinders.dataSegmentPullerBinder(binder)
                .addBinding("hdfs")
                .to(HdfsDataSegmentPuller.class)
                .in(LazySingleton.class);
    binder.bind(Configuration.class).toInstance(new Configuration());
  }

  private static void bindDeepStorageCassandra(Binder binder)
  {
    DruidBinders.dataSegmentPullerBinder(binder)
                .addBinding("c*")
                .to(CassandraDataSegmentPuller.class)
                .in(LazySingleton.class);
    ConfigProvider.bind(binder, CassandraDataSegmentConfig.class);
  }
}
