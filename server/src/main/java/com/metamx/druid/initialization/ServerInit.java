/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.initialization;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.DruidProcessingConfig;
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
import com.metamx.druid.query.timeseries.TimeseriesQuery;
import com.metamx.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.skife.config.ConfigurationObjectFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ServerInit
{
  private static Logger log = new Logger(ServerInit.class);

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

  public static StupidPool<ByteBuffer> makeComputeScratchPool(DruidProcessingConfig config)
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
          throw new ISE(
              "Not enough direct memory.  Please adjust -XX:MaxDirectMemory or druid.computation.buffer.size: "
              + "maxDirectMemory[%,d], memoryNeeded[%,d], druid.computation.buffer.size[%,d], numThreads[%,d]",
              maxDirectMemory, memoryNeeded, config.intermediateComputeSizeBytes(), config.getNumThreads()
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

    return new ComputeScratchPool(config.intermediateComputeSizeBytes());
  }

  public static Map<Class<? extends Query>, QueryRunnerFactory> initDefaultQueryTypes(
      ConfigurationObjectFactory configFactory,
      StupidPool<ByteBuffer> computationBufferPool
  )
  {
    Map<Class<? extends Query>, QueryRunnerFactory> queryRunners = Maps.newLinkedHashMap();
    queryRunners.put(TimeseriesQuery.class, new TimeseriesQueryRunnerFactory());
    queryRunners.put(
        GroupByQuery.class,
        new GroupByQueryRunnerFactory(
            new GroupByQueryEngine(
                configFactory.build(GroupByQueryEngineConfig.class),
                computationBufferPool
            )
        )
    );
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
