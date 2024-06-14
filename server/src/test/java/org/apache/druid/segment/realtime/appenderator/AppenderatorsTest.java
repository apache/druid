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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.NoopDataSegmentPusher;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Map;


public class AppenderatorsTest
{
  @Test
  public void testOpenSegmentsOfflineAppenderator() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester("OPEN_SEGMENTS")) {
      Assert.assertTrue(tester.appenderator instanceof AppenderatorImpl);
      AppenderatorImpl appenderator = (AppenderatorImpl) tester.appenderator;
      Assert.assertTrue(appenderator.isOpenSegments());
    }
  }

  @Test
  public void testClosedSegmentsOfflineAppenderator() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester("CLOSED_SEGMENTS")) {
      Assert.assertTrue(tester.appenderator instanceof AppenderatorImpl);
      AppenderatorImpl appenderator = (AppenderatorImpl) tester.appenderator;
      Assert.assertFalse(appenderator.isOpenSegments());
    }
  }

  @Test
  public void testClosedSegmentsSinksOfflineAppenderator() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester("CLOSED_SEGMENTS_SINKS")) {
      Assert.assertTrue(tester.appenderator instanceof BatchAppenderator);
    }
  }

  private static class AppenderatorTester implements AutoCloseable
  {
    public static final String DATASOURCE = "foo";

    private final AppenderatorConfig tuningConfig;
    private final Appenderator appenderator;
    private final ServiceEmitter emitter;

    public AppenderatorTester(final String batchMode)
    {
      this(100, 100, null, new SimpleRowIngestionMeters(), false, batchMode);
    }

    public AppenderatorTester(
        final int maxRowsInMemory,
        final long maxSizeInBytes,
        @Nullable final File basePersistDirectory,
        final RowIngestionMeters rowIngestionMeters,
        final boolean skipBytesInMemoryOverheadCheck,
        String batchMode
    )
    {
      ObjectMapper objectMapper = new DefaultObjectMapper();
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

      DataSchema schema = new DataSchema(
          DATASOURCE,
          null,
          null,
          new AggregatorFactory[]{
              new CountAggregatorFactory("count"),
              new LongSumAggregatorFactory("met", "met")
          },
          new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null),
          null,
          parserMap,
          objectMapper
      );

      tuningConfig = new TestAppenderatorConfig(
          TuningConfig.DEFAULT_APPENDABLE_INDEX,
          maxRowsInMemory,
          maxSizeInBytes == 0L ? getDefaultMaxBytesInMemory() : maxSizeInBytes,
          skipBytesInMemoryOverheadCheck,
          IndexSpec.DEFAULT,
          0,
          false,
          0L,
          OffHeapMemorySegmentWriteOutMediumFactory.instance(),
          IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE,
          basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory
      );
      SegmentGenerationMetrics metrics = new SegmentGenerationMetrics();

      IndexIO indexIO = new IndexIO(objectMapper, ColumnConfig.DEFAULT);
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

      switch (batchMode) {
        case "OPEN_SEGMENTS":
          appenderator = Appenderators.createOpenSegmentsOffline(
              schema.getDataSource(),
              schema,
              tuningConfig,
              metrics,
              new NoopDataSegmentPusher(),
              objectMapper,
              indexIO,
              indexMerger,
              rowIngestionMeters,
              new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
              false,
              CentralizedDatasourceSchemaConfig.create()
          );
          break;
        case "CLOSED_SEGMENTS":
          appenderator = Appenderators.createClosedSegmentsOffline(
              schema.getDataSource(),
              schema,
              tuningConfig,
              metrics,
              new NoopDataSegmentPusher(),
              objectMapper,
              indexIO,
              indexMerger,
              rowIngestionMeters,
              new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
              false,
              CentralizedDatasourceSchemaConfig.create()
          );

          break;
        case "CLOSED_SEGMENTS_SINKS":
          appenderator = Appenderators.createOffline(
              schema.getDataSource(),
              schema,
              tuningConfig,
              metrics,
              new NoopDataSegmentPusher(),
              objectMapper,
              indexIO,
              indexMerger,
              rowIngestionMeters,
              new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
              false,
              CentralizedDatasourceSchemaConfig.create()
          );
          break;
        default:
          throw new IllegalArgumentException("Unrecognized batchMode: " + batchMode);
      }
    }

    private long getDefaultMaxBytesInMemory()
    {
      return (Runtime.getRuntime().totalMemory()) / 3;
    }

    @Override
    public void close() throws Exception
    {
      appenderator.close();
      emitter.close();
      FileUtils.deleteDirectory(tuningConfig.getBasePersistDirectory());
    }

    private static File createNewBasePersistDirectory()
    {
      return FileUtils.createTempDir("druid-batch-persist");
    }
  }
}
