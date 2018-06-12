/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.core.NoopEmitter;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

public class AppenderatorTester implements AutoCloseable
{
  public static final String DATASOURCE = "foo";

  private final DataSchema schema;
  private final RealtimeTuningConfig tuningConfig;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final Appenderator appenderator;
  private final ExecutorService queryExecutor;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final ServiceEmitter emitter;

  private final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<>();

  public AppenderatorTester(
      final int maxRowsInMemory
  )
  {
    this(maxRowsInMemory, -1, null, false);
  }

  public AppenderatorTester(
      final int maxRowsInMemory,
      final boolean enablePushFailure
  )
  {
    this(maxRowsInMemory, -1, null, enablePushFailure);
  }

  public AppenderatorTester(
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      final boolean enablePushFailure
  )
  {
    this(maxRowsInMemory, maxSizeInBytes, null, enablePushFailure);
  }

  public AppenderatorTester(
      final int maxRowsInMemory,
      long maxSizeInBytes,
      final File basePersistDirectory,
      final boolean enablePushFailure
  )
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(LinearShardSpec.class);

    final Map<String, Object> parserMap = objectMapper.convertValue(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("ts", "auto", null),
                new DimensionsSpec(null, null, null),
                null,
                null
            )
        ),
        Map.class
    );
    schema = new DataSchema(
        DATASOURCE,
        parserMap,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("met", "met")
        },
        new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null),
        null,
        objectMapper
    );
    maxSizeInBytes = maxSizeInBytes == 0L ? getDefaultMaxBytesInMemory() : maxSizeInBytes;
    tuningConfig = new RealtimeTuningConfig(
        maxRowsInMemory,
        maxSizeInBytes,
        null,
        null,
        basePersistDirectory,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        null
    );

    metrics = new FireDepartmentMetrics();
    queryExecutor = Execs.singleThreaded("queryExecutor(%d)");

    indexIO = new IndexIO(
        objectMapper,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    indexMerger = new IndexMergerV9(objectMapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    emitter = new ServiceEmitter(
        "test",
        "test",
        new NoopEmitter()
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);
    dataSegmentPusher = new DataSegmentPusher()
    {
      private boolean mustFail = true;

      @Deprecated
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return getPathForHadoop();
      }

      @Override
      public String getPathForHadoop()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataSegment push(File file, DataSegment segment, boolean useUniquePath) throws IOException
      {
        if (enablePushFailure && mustFail) {
          mustFail = false;
          throw new IOException("Push failure test");
        } else if (enablePushFailure) {
          mustFail = true;
        }
        pushedSegments.add(segment);
        return segment;
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };
    appenderator = Appenderators.createRealtime(
        schema,
        tuningConfig,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger,
        new DefaultQueryRunnerFactoryConglomerate(
            ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
                TimeseriesQuery.class, new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(
                        new IntervalChunkingQueryRunnerDecorator(
                            queryExecutor,
                            QueryRunnerTestHelper.NOOP_QUERYWATCHER,
                            emitter
                        )
                    ),
                    new TimeseriesQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
        ),
        new DataSegmentAnnouncer()
        {
          @Override
          public void announceSegment(DataSegment segment)
          {

          }

          @Override
          public void unannounceSegment(DataSegment segment)
          {

          }

          @Override
          public void announceSegments(Iterable<DataSegment> segments)
          {

          }

          @Override
          public void unannounceSegments(Iterable<DataSegment> segments)
          {

          }
        },
        emitter,
        queryExecutor,
        MapCache.create(2048),
        new CacheConfig()
    );
  }

  private long getDefaultMaxBytesInMemory()
  {
    return (Runtime.getRuntime().totalMemory()) / 3;
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public RealtimeTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }

  public DataSegmentPusher getDataSegmentPusher()
  {
    return dataSegmentPusher;
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Appenderator getAppenderator()
  {
    return appenderator;
  }

  public List<DataSegment> getPushedSegments()
  {
    return pushedSegments;
  }

  @Override
  public void close() throws Exception
  {
    appenderator.close();
    queryExecutor.shutdownNow();
    emitter.close();
    FileUtils.deleteDirectory(tuningConfig.getBasePersistDirectory());
  }
}
