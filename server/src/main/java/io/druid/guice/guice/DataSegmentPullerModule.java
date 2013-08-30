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

package io.druid.guice.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.metamx.druid.loading.DataSegmentPuller;
import com.metamx.druid.loading.HdfsDataSegmentPuller;
import com.metamx.druid.loading.LocalDataSegmentPuller;
import com.metamx.druid.loading.OmniSegmentLoader;
import com.metamx.druid.loading.S3DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentConfig;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentPuller;
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
    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder, String.class, DataSegmentPuller.class
    );
    segmentPullerBinder.addBinding("local").to(LocalDataSegmentPuller.class).in(LazySingleton.class);
  }

  private static void bindDeepStorageS3(Binder binder)
  {
    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder, String.class, DataSegmentPuller.class
    );
    segmentPullerBinder.addBinding("s3_zip").to(S3DataSegmentPuller.class).in(LazySingleton.class);
  }

  private static void bindDeepStorageHdfs(Binder binder)
  {
    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder, String.class, DataSegmentPuller.class
    );
    segmentPullerBinder.addBinding("hdfs").to(HdfsDataSegmentPuller.class).in(LazySingleton.class);
    binder.bind(Configuration.class).toInstance(new Configuration());
  }

  private static void bindDeepStorageCassandra(Binder binder)
  {
    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder, String.class, DataSegmentPuller.class
    );
    segmentPullerBinder.addBinding("c*").to(CassandraDataSegmentPuller.class).in(LazySingleton.class);
    ConfigProvider.bind(binder, CassandraDataSegmentConfig.class);
  }
}
