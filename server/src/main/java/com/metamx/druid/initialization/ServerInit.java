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

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.skife.config.ConfigurationObjectFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.DruidProcessingConfig;
import com.metamx.druid.Query;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.DelegatingSegmentLoader;
import com.metamx.druid.loading.HdfsDataSegmentPuller;
import com.metamx.druid.loading.HdfsDataSegmentPusher;
import com.metamx.druid.loading.HdfsDataSegmentPusherConfig;
import com.metamx.druid.loading.LocalDataSegmentPuller;
import com.metamx.druid.loading.LocalDataSegmentPusher;
import com.metamx.druid.loading.LocalDataSegmentPusherConfig;
import com.metamx.druid.loading.MMappedQueryableIndexFactory;
import com.metamx.druid.loading.QueryableIndexFactory;
import com.metamx.druid.loading.S3DataSegmentPuller;
import com.metamx.druid.loading.S3DataSegmentPusher;
import com.metamx.druid.loading.S3DataSegmentPusherConfig;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.SegmentLoaderConfig;
import com.metamx.druid.loading.SingleSegmentLoader;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentConfig;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentPuller;
import com.metamx.druid.loading.cassandra.CassandraDataSegmentPusher;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.group.GroupByQueryEngine;
import com.metamx.druid.query.group.GroupByQueryEngineConfig;
import com.metamx.druid.query.group.GroupByQueryRunnerFactory;
import com.metamx.druid.query.group.GroupByQueryRunnerFactoryConfig;
import com.metamx.druid.query.metadata.SegmentMetadataQuery;
import com.metamx.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.search.SearchQueryRunnerFactory;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import com.metamx.druid.query.timeseries.TimeseriesQuery;
import com.metamx.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import com.metamx.druid.utils.PropUtils;

/**
 */
public class ServerInit
{
  private static Logger log = new Logger(ServerInit.class);

  public static SegmentLoader makeDefaultQueryableLoader(
  		final ConfigurationObjectFactory configFactory,
  		final Properties props
  ) throws S3ServiceException
  {
  	SegmentLoaderConfig config = configFactory.build(SegmentLoaderConfig.class);
    DelegatingSegmentLoader delegateLoader = new DelegatingSegmentLoader();
    final QueryableIndexFactory factory = new MMappedQueryableIndexFactory();

    final RestS3Service s3Client = new RestS3Service(
        new AWSCredentials(
            props.getProperty("com.metamx.aws.accessKey", ""),
            props.getProperty("com.metamx.aws.secretKey", "")
        )
    );
    final S3DataSegmentPuller segmentGetter = new S3DataSegmentPuller(s3Client);
    final SingleSegmentLoader s3segmentLoader = new SingleSegmentLoader(segmentGetter, factory, config);
    
    delegateLoader.setLoaderTypes(
        ImmutableMap.<String, SegmentLoader>builder()
        .put("local", new SingleSegmentLoader(new LocalDataSegmentPuller(), factory, config))
        .put("hdfs", new SingleSegmentLoader(new HdfsDataSegmentPuller(new Configuration()), factory, config))
        .put("s3", s3segmentLoader)
        .put("s3_zip", s3segmentLoader)
        .put("c*",new SingleSegmentLoader(new CassandraDataSegmentPuller(configFactory.build(CassandraDataSegmentConfig.class)), factory, config))
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
              "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize or druid.computation.buffer.size: "
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
            ),
            configFactory.build(GroupByQueryRunnerFactoryConfig.class)
        )
    );
    queryRunners.put(SearchQuery.class, new SearchQueryRunnerFactory());
    queryRunners.put(TimeBoundaryQuery.class, new TimeBoundaryQueryRunnerFactory());
    queryRunners.put(SegmentMetadataQuery.class, new SegmentMetadataQueryRunnerFactory());
    return queryRunners;
  }

  public static DataSegmentPusher getSegmentPusher(
      final Properties props,
      final ConfigurationObjectFactory configFactory,
      final ObjectMapper jsonMapper
  )
  {
    if (Boolean.parseBoolean(props.getProperty("druid.pusher.local", "false"))) {
      return new LocalDataSegmentPusher(configFactory.build(LocalDataSegmentPusherConfig.class), jsonMapper);
    }
    else if (Boolean.parseBoolean(props.getProperty("druid.pusher.cassandra", "false"))) {
      final CassandraDataSegmentConfig config = configFactory.build(CassandraDataSegmentConfig.class);

      return new CassandraDataSegmentPusher(config, jsonMapper);
    }
    else if (Boolean.parseBoolean(props.getProperty("druid.pusher.hdfs", "false"))) {
      final HdfsDataSegmentPusherConfig config = configFactory.build(HdfsDataSegmentPusherConfig.class);

      return new HdfsDataSegmentPusher(config, new Configuration(), jsonMapper);
    }
    else {

      final RestS3Service s3Client;
      try {
        s3Client = new RestS3Service(
            new AWSCredentials(
                PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
                PropUtils.getProperty(props, "com.metamx.aws.secretKey")
            )
        );
      }
      catch (S3ServiceException e) {
        throw Throwables.propagate(e);
      }

      return new S3DataSegmentPusher(s3Client, configFactory.build(S3DataSegmentPusherConfig.class), jsonMapper);
    }
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
