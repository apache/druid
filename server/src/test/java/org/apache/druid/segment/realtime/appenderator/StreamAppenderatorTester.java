/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

public class StreamAppenderatorTester implements AutoCloseable
{
  public static final String DATASOURCE = "foo";

  private final DataSchema schema;
  private final RealtimeTuningConfig tuningConfig;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final Appenderator appenderator;
  private final ExecutorService queryExecutor;
  private final ServiceEmitter emitter;

  private final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<>();

  public StreamAppenderatorTester(
      final int delayInMilli,
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      final File basePersistDirectory,
      final boolean enablePushFailure,
      final RowIngestionMeters rowIngestionMeters,
      final boolean skipBytesInMemoryOverheadCheck,
      final DataSegmentAnnouncer announcer,
      final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(LinearShardSpec.class);

    final Map<String, Object> parserMap = objectMapper.convertValue(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("ts", "auto", null),
                DimensionsSpec.EMPTY,
                null,
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
    tuningConfig = new RealtimeTuningConfig(
        null,
        maxRowsInMemory,
        maxSizeInBytes == 0L ? getDefaultMaxBytesInMemory() : maxSizeInBytes,
        skipBytesInMemoryOverheadCheck,
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
        null,
        null
    );

    metrics = new FireDepartmentMetrics();
    queryExecutor = Execs.singleThreaded("queryExecutor(%d)");

    IndexIO indexIO = new IndexIO(
        objectMapper,
        new ColumnConfig()
        {
        }
    );

    IndexMergerV9 indexMerger = new IndexMergerV9(
        objectMapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

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

    if (delayInMilli <= 0) {
      appenderator = Appenderators.createRealtime(
          null,
          schema.getDataSource(),
          schema,
          tuningConfig,
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          indexMerger,
          new DefaultQueryRunnerFactoryConglomerate(
              ImmutableMap.of(
                  TimeseriesQuery.class, new TimeseriesQueryRunnerFactory(
                      new TimeseriesQueryQueryToolChest(),
                      new TimeseriesQueryEngine(),
                      QueryRunnerTestHelper.NOOP_QUERYWATCHER
                  ),
                  ScanQuery.class, new ScanQueryRunnerFactory(
                      new ScanQueryQueryToolChest(
                          new ScanQueryConfig(),
                          new DefaultGenericQueryMetricsFactory()
                      ),
                      new ScanQueryEngine(),
                      new ScanQueryConfig()
                  )
              )
          ),
          announcer,
          emitter,
          new ForwardingQueryProcessingPool(queryExecutor),
          MapCache.create(2048),
          new CacheConfig(),
          new CachePopulatorStats(),
          rowIngestionMeters,
          new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
          true,
          centralizedDatasourceSchemaConfig
      );
    } else {
      SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
      {
        @Override
        public int getDropSegmentDelayMillis()
        {
          return delayInMilli;
        }
      };
      appenderator = Appenderators.createRealtime(
          segmentLoaderConfig,
          schema.getDataSource(),
          schema,
          tuningConfig,
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          indexMerger,
          new DefaultQueryRunnerFactoryConglomerate(
              ImmutableMap.of(
                  TimeseriesQuery.class, new TimeseriesQueryRunnerFactory(
                      new TimeseriesQueryQueryToolChest(),
                      new TimeseriesQueryEngine(),
                      QueryRunnerTestHelper.NOOP_QUERYWATCHER
                  ),
                  ScanQuery.class, new ScanQueryRunnerFactory(
                      new ScanQueryQueryToolChest(
                          new ScanQueryConfig(),
                          new DefaultGenericQueryMetricsFactory()
                      ),
                      new ScanQueryEngine(),
                      new ScanQueryConfig()
                  )
              )
          ),
          new NoopDataSegmentAnnouncer(),
          emitter,
          new ForwardingQueryProcessingPool(queryExecutor),
          MapCache.create(2048),
          new CacheConfig(),
          new CachePopulatorStats(),
          rowIngestionMeters,
          new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
          true,
          centralizedDatasourceSchemaConfig
      );
    }
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

  public static class Builder
  {
    private int maxRowsInMemory;
    private long maxSizeInBytes = -1;
    private File basePersistDirectory;
    private boolean enablePushFailure;
    private RowIngestionMeters rowIngestionMeters;
    private boolean skipBytesInMemoryOverheadCheck;
    private int delayInMilli = 0;

    public Builder maxRowsInMemory(final int maxRowsInMemory)
    {
      this.maxRowsInMemory = maxRowsInMemory;
      return this;
    }

    public Builder maxSizeInBytes(final long maxSizeInBytes)
    {
      this.maxSizeInBytes = maxSizeInBytes;
      return this;
    }

    public Builder basePersistDirectory(final File basePersistDirectory)
    {
      this.basePersistDirectory = basePersistDirectory;
      return this;
    }

    public Builder enablePushFailure(final boolean enablePushFailure)
    {
      this.enablePushFailure = enablePushFailure;
      return this;
    }

    public Builder rowIngestionMeters(final RowIngestionMeters rowIngestionMeters)
    {
      this.rowIngestionMeters = rowIngestionMeters;
      return this;
    }

    public Builder skipBytesInMemoryOverheadCheck(final boolean skipBytesInMemoryOverheadCheck)
    {
      this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
      return this;
    }

    public Builder withSegmentDropDelayInMilli(int delayInMilli)
    {
      this.delayInMilli = delayInMilli;
      return this;
    }

    public StreamAppenderatorTester build()
    {
      return new StreamAppenderatorTester(
          delayInMilli,
          maxRowsInMemory,
          maxSizeInBytes,
          Preconditions.checkNotNull(basePersistDirectory, "basePersistDirectory"),
          enablePushFailure,
          rowIngestionMeters == null ? new SimpleRowIngestionMeters() : rowIngestionMeters,
          skipBytesInMemoryOverheadCheck,
          new NoopDataSegmentAnnouncer(),
          CentralizedDatasourceSchemaConfig.create()
      );
    }

    public StreamAppenderatorTester build(
        DataSegmentAnnouncer dataSegmentAnnouncer,
        CentralizedDatasourceSchemaConfig config
    )
    {
      return new StreamAppenderatorTester(
          delayInMilli,
          maxRowsInMemory,
          maxSizeInBytes,
          Preconditions.checkNotNull(basePersistDirectory, "basePersistDirectory"),
          enablePushFailure,
          rowIngestionMeters == null ? new SimpleRowIngestionMeters() : rowIngestionMeters,
          skipBytesInMemoryOverheadCheck,
          dataSegmentAnnouncer,
          config
      );
    }
  }
}
