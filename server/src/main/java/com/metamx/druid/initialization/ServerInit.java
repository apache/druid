package com.metamx.druid.initialization;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.GroupByQueryEngine;
import com.metamx.druid.GroupByQueryEngineConfig;
import com.metamx.druid.Query;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.loading.DelegatingStorageAdapterLoader;
import com.metamx.druid.loading.MMappedStorageAdapterFactory;
import com.metamx.druid.loading.QueryableLoaderConfig;
import com.metamx.druid.loading.RealtimeSegmentGetter;
import com.metamx.druid.loading.S3SegmentGetter;
import com.metamx.druid.loading.S3ZippedSegmentGetter;
import com.metamx.druid.loading.SingleStorageAdapterLoader;
import com.metamx.druid.loading.StorageAdapterFactory;
import com.metamx.druid.loading.StorageAdapterLoader;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.group.GroupByQueryRunnerFactory;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.search.SearchQueryRunnerFactory;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.skife.config.ConfigurationObjectFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ServerInit
{
  public static StorageAdapterLoader makeDefaultQueryableLoader(
      RestS3Service s3Client,
      QueryableLoaderConfig config
  )
  {
    DelegatingStorageAdapterLoader delegateLoader = new DelegatingStorageAdapterLoader();

    final S3SegmentGetter segmentGetter = new S3SegmentGetter(s3Client, config);
    final S3ZippedSegmentGetter zippedGetter = new S3ZippedSegmentGetter(s3Client, config);
    final RealtimeSegmentGetter realtimeGetter = new RealtimeSegmentGetter(config);
    final StorageAdapterFactory factory;
    if ("mmap".equals(config.getQueryableFactoryType())) {
      factory = new MMappedStorageAdapterFactory();
    } else {
      throw new ISE("Unknown queryableFactoryType[%s]", config.getQueryableFactoryType());
    }

    delegateLoader.setLoaderTypes(
        ImmutableMap.<String, StorageAdapterLoader>builder()
                    .put("s3", new SingleStorageAdapterLoader(segmentGetter, factory))
                    .put("s3_zip", new SingleStorageAdapterLoader(zippedGetter, factory))
                    .put("realtime", new SingleStorageAdapterLoader(realtimeGetter, factory))
                    .build()
    );

    return delegateLoader;
  }

  public static StupidPool<ByteBuffer> makeComputeScratchPool(int computationBufferSize)
  {
    return new ComputeScratchPool(computationBufferSize);
  }

  public static Map<Class<? extends Query>, QueryRunnerFactory> initDefaultQueryTypes(
      ConfigurationObjectFactory configFactory,
      StupidPool<ByteBuffer> computationBufferPool
  )
  {
    Map<Class<? extends Query>, QueryRunnerFactory> queryRunners = Maps.newLinkedHashMap();
    queryRunners.put(GroupByQuery.class, new GroupByQueryRunnerFactory(new GroupByQueryEngine(configFactory.build(GroupByQueryEngineConfig.class), computationBufferPool)));
    queryRunners.put(SearchQuery.class, new SearchQueryRunnerFactory());
    queryRunners.put(TimeBoundaryQuery.class, new TimeBoundaryQueryRunnerFactory());
    return queryRunners;
  }

  private static class ComputeScratchPool extends StupidPool<ByteBuffer>
  {
    private static final Logger log = new Logger(ComputeScratchPool.class);

    public ComputeScratchPool(final int computationBufferSize)
    {
      super(
          new Supplier<ByteBuffer>()
          {
            final AtomicLong count = new AtomicLong(0);

            @Override
            public ByteBuffer get()
            {
              log.info(
                  "Allocating new computeScratchPool[%,d] of size[%,d]", count.getAndIncrement(), computationBufferSize
              );
              return ByteBuffer.allocateDirect(computationBufferSize);
            }
          }
      );
    }
  }
}
