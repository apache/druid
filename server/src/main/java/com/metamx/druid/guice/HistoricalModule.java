package com.metamx.druid.guice;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.google.inject.multibindings.MapBinder;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.logger.Logger;
import com.metamx.druid.DruidProcessingConfig;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.coordination.BatchDataSegmentAnnouncer;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.coordination.DataSegmentAnnouncerProvider;
import com.metamx.druid.coordination.DruidServerMetadata;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.druid.coordination.SingleDataSegmentAnnouncer;
import com.metamx.druid.coordination.ZkCoordinator;
import com.metamx.druid.curator.announcement.Announcer;
import com.metamx.druid.guice.annotations.Global;
import com.metamx.druid.guice.annotations.Processing;
import com.metamx.druid.guice.annotations.Self;
import com.metamx.druid.initialization.BatchDataSegmentAnnouncerConfig;
import com.metamx.druid.initialization.DruidNode;
import com.metamx.druid.loading.DataSegmentPuller;
import com.metamx.druid.loading.HdfsDataSegmentPuller;
import com.metamx.druid.loading.LocalDataSegmentPuller;
import com.metamx.druid.loading.MMappedQueryableIndexFactory;
import com.metamx.druid.loading.OmniSegmentLoader;
import com.metamx.druid.loading.QueryableIndexFactory;
import com.metamx.druid.loading.S3CredentialsConfig;
import com.metamx.druid.loading.S3DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.SegmentLoaderConfig;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentConfig;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentPuller;
import com.metamx.druid.query.DefaultQueryRunnerFactoryConglomerate;
import com.metamx.druid.query.MetricsEmittingExecutorService;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class HistoricalModule implements Module
{
  private static final Logger log = new Logger(HistoricalModule.class);

  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, DruidProcessingConfig.class, ImmutableMap.of("base_path", "druid.processing"));
    binder.bind(ExecutorServiceConfig.class).to(DruidProcessingConfig.class);

    JsonConfigProvider.bind(binder, "druid.server", DruidServerConfig.class);
    JsonConfigProvider.bind(binder, "druid.segmentCache", SegmentLoaderConfig.class);

    binder.bind(ServerManager.class).in(LazySingleton.class);

    binder.bind(SegmentLoader.class).to(OmniSegmentLoader.class).in(LazySingleton.class);
    binder.bind(QueryableIndexFactory.class).to(MMappedQueryableIndexFactory.class).in(LazySingleton.class);

    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder, String.class, DataSegmentPuller.class
    );
    segmentPullerBinder.addBinding("local").to(LocalDataSegmentPuller.class).in(LazySingleton.class);

    bindDeepStorageS3(binder);
    bindDeepStorageHdfs(binder);
    bindDeepStorageCassandra(binder);

    binder.bind(QueryRunnerFactoryConglomerate.class)
          .to(DefaultQueryRunnerFactoryConglomerate.class)
          .in(LazySingleton.class);

    binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);

    JsonConfigProvider.bind(binder, "druid.announcer", BatchDataSegmentAnnouncerConfig.class);
    JsonConfigProvider.bind(binder, "druid.announcer", DataSegmentAnnouncerProvider.class);
    binder.bind(DataSegmentAnnouncer.class).toProvider(DataSegmentAnnouncerProvider.class);
    binder.bind(BatchDataSegmentAnnouncer.class).in(ManageLifecycleLast.class);
    binder.bind(SingleDataSegmentAnnouncer.class).in(ManageLifecycleLast.class);
  }

  @Provides @LazySingleton
  public DruidServerMetadata getMetadata(@Self DruidNode node, DruidServerConfig config)
  {
    return new DruidServerMetadata(node.getHost(), node.getHost(), config.getMaxSize(), "historical", config.getTier());
  }

  @Provides @ManageLifecycle
  public Announcer getAnnouncer(CuratorFramework curator)
  {
    return new Announcer(curator, Execs.singleThreaded("Announcer-%s"));
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

  @Provides @LazySingleton
  public RestS3Service getRestS3Service(S3CredentialsConfig config)
  {
    try {
      return new RestS3Service(new AWSCredentials(config.getAccessKey(), config.getSecretKey()));
    }
    catch (S3ServiceException e) {
      throw new ProvisionException("Unable to create a RestS3Service", e);
    }
  }

  @Provides @LazySingleton @Global
  public StupidPool<ByteBuffer> getIntermediateResultsPool(DruidProcessingConfig config)
  {
    try {
      Class<?> vmClass = Class.forName("sun.misc.VM");
      Object maxDirectMemoryObj = vmClass.getMethod("maxDirectMemory").invoke(null);

      if (maxDirectMemoryObj == null || !(maxDirectMemoryObj instanceof Number)) {
        log.info("Cannot determine maxDirectMemory from[%s]", maxDirectMemoryObj);
      } else {
        long maxDirectMemory = ((Number) maxDirectMemoryObj).longValue();

        final long memoryNeeded = (long) config.intermediateComputeSizeBytes() * (config.getNumThreads() + 1);
        if (maxDirectMemory < memoryNeeded) {
          throw new ProvisionException(
              String.format(
                  "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize or druid.computation.buffer.size: "
                  + "maxDirectMemory[%,d], memoryNeeded[%,d], druid.computation.buffer.size[%,d], numThreads[%,d]",
                  maxDirectMemory, memoryNeeded, config.intermediateComputeSizeBytes(), config.getNumThreads()
              )
          );
        }
      }
    }
    catch (ClassNotFoundException e) {
      log.info("No VM class, cannot do memory check.");
    }
    catch (NoSuchMethodException e) {
      log.info("VM.maxDirectMemory doesn't exist, cannot do memory check.");
    }
    catch (InvocationTargetException e) {
      log.warn(e, "static method shouldn't throw this");
    }
    catch (IllegalAccessException e) {
      log.warn(e, "public method, shouldn't throw this");
    }

    return new IntermediateProcessingBufferPool(config.intermediateComputeSizeBytes());
  }

  private static void bindDeepStorageS3(Binder binder)
  {
    final MapBinder<String, DataSegmentPuller> segmentPullerBinder = MapBinder.newMapBinder(
        binder, String.class, DataSegmentPuller.class
    );
    segmentPullerBinder.addBinding("s3_zip").to(S3DataSegmentPuller.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.s3", S3CredentialsConfig.class);
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

  private static class IntermediateProcessingBufferPool extends StupidPool<ByteBuffer>
  {
    private static final Logger log = new Logger(IntermediateProcessingBufferPool.class);

    public IntermediateProcessingBufferPool(final int computationBufferSize)
    {
      super(
          new Supplier<ByteBuffer>()
          {
            final AtomicLong count = new AtomicLong(0);

            @Override
            public ByteBuffer get()
            {
              log.info(
                  "Allocating new intermediate processing buffer[%,d] of size[%,d]",
                  count.getAndIncrement(), computationBufferSize
              );
              return ByteBuffer.allocateDirect(computationBufferSize);
            }
          }
      );
    }
  }
}
