package com.metamx.druid.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.druid.guice.annotations.Processing;
import com.metamx.druid.loading.BaseSegmentLoader;
import com.metamx.druid.loading.DataSegmentPuller;
import com.metamx.druid.loading.HdfsDataSegmentPuller;
import com.metamx.druid.loading.LocalDataSegmentPuller;
import com.metamx.druid.loading.MMappedQueryableIndexFactory;
import com.metamx.druid.loading.QueryableIndexFactory;
import com.metamx.druid.loading.S3DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.SegmentLoaderConfig;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentPuller;
import com.metamx.druid.query.MetricsEmittingExecutorService;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class HistoricalModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, DruidServerConfig.class);
    ConfigProvider.bind(binder, ExecutorServiceConfig.class, ImmutableMap.of("base_path", "druid.processing"));

    JsonConfigProvider.bind(binder, "druid.segmentCache", SegmentLoaderConfig.class);

    binder.bind(ServerManager.class).in(LazySingleton.class);

    binder.bind(SegmentLoader.class).to(BaseSegmentLoader.class).in(LazySingleton.class);
    binder.bind(QueryableIndexFactory.class).to(MMappedQueryableIndexFactory.class).in(LazySingleton.class);

    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder,
        String.class,
        DataSegmentPuller.class
    );

    segmentPullerBinder.addBinding("local").to(LocalDataSegmentPuller.class).in(LazySingleton.class);
    segmentPullerBinder.addBinding("hdfs").to(HdfsDataSegmentPuller.class).in(LazySingleton.class);
    segmentPullerBinder.addBinding("s3_zip").to(S3DataSegmentPuller.class).in(LazySingleton.class);
    segmentPullerBinder.addBinding("c*").to(CassandraDataSegmentPuller.class).in(LazySingleton.class);
  }

  @Provides @Processing @ManageLifecycle
  public ExecutorService getProcessingExecutorService(ExecutorServiceConfig config, ServiceEmitter emitter)
  {
    return new MetricsEmittingExecutorService(
        Executors.newFixedThreadPool(config.getNumThreads(), Execs.makeThreadFactory(config.getFormatString())),
        emitter,
        new ServiceMetricEvent.Builder()
    );
  }
}
