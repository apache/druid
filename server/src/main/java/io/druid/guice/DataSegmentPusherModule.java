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
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.HdfsDataSegmentPusherConfig;
import io.druid.segment.loading.S3DataSegmentPusherConfig;
import io.druid.segment.loading.cassandra.CassandraDataSegmentConfig;
import org.apache.hadoop.conf.Configuration;

/**
 */
public class DataSegmentPusherModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.pusher", DataSegmentPusherProvider.class);

    JsonConfigProvider.bind(binder, "druid.pusher.s3", S3DataSegmentPusherConfig.class);
    binder.bind(Configuration.class).toInstance(new Configuration());

    JsonConfigProvider.bind(binder, "druid.pusher.hdfs", HdfsDataSegmentPusherConfig.class);

    JsonConfigProvider.bind(binder, "druid.pusher.cassandra", CassandraDataSegmentConfig.class);

    binder.bind(DataSegmentPusher.class).toProvider(DataSegmentPusherProvider.class);
  }
}
