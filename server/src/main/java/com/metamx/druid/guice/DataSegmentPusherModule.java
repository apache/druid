package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.HdfsDataSegmentPusherConfig;
import com.metamx.druid.loading.LocalDataSegmentPusherConfig;
import com.metamx.druid.loading.S3DataSegmentPusherConfig;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentConfig;
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
