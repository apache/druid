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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
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
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorImpl;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderator;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;


public class BatchAppenderatorsTest
{
  @Test
  public void testLegacyOfflineAppenderator() throws Exception
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

    private final DataSchema schema;
    private final AppenderatorConfig tuningConfig;
    private final FireDepartmentMetrics metrics;
    private final ObjectMapper objectMapper;
    private final Appenderator appenderator;
    private final ServiceEmitter emitter;

    private final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<>();


    public AppenderatorTester(
        final String batchMode
    )
    {
      this(100, 100, null, false, new SimpleRowIngestionMeters(),
           false, batchMode
      );
    }

    public AppenderatorTester(
        final int maxRowsInMemory,
        final long maxSizeInBytes,
        @Nullable final File basePersistDirectory,
        final boolean enablePushFailure,
        final RowIngestionMeters rowIngestionMeters,
        final boolean skipBytesInMemoryOverheadCheck,
        String batchMode
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

      tuningConfig = new TestIndexTuningConfig(
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
          basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory,
          null
      );
      metrics = new FireDepartmentMetrics();

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
      DataSegmentPusher dataSegmentPusher = new DataSegmentPusher()
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

      switch (batchMode) {
        case "OPEN_SEGMENTS":
          appenderator = BatchAppenderators.newAppenderator(
              "foo",
              new TestAppenderatorsManager(),
              metrics,
              makeTaskToolbox(
                  objectMapper,
                  indexMerger,
                  TaskConfig.BatchProcessingMode.OPEN_SEGMENTS
              ),
              schema,
              tuningConfig,
              dataSegmentPusher,
              rowIngestionMeters,
              new ParseExceptionHandler(
                  rowIngestionMeters,
                  false,
                  Integer.MAX_VALUE,
                  0
              ),
              false
          );
          break;
        case "CLOSED_SEGMENTS":
          appenderator = BatchAppenderators.newAppenderator(
              "foo",
              new TestAppenderatorsManager(),
              metrics,
              makeTaskToolbox(
                  objectMapper,
                  indexMerger,
                  TaskConfig.BatchProcessingMode.CLOSED_SEGMENTS
              ),
              schema,
              tuningConfig,
              dataSegmentPusher,
              rowIngestionMeters,
              new ParseExceptionHandler(
                  rowIngestionMeters,
                  false,
                  Integer.MAX_VALUE,
                  0
              ),
              false
          );

          break;
        case "CLOSED_SEGMENTS_SINKS":
          appenderator = BatchAppenderators.newAppenderator(
              "foo",
              new TestAppenderatorsManager(),
              metrics,
              makeTaskToolbox(
                  objectMapper,
                  indexMerger,
                  TaskConfig.BatchProcessingMode.CLOSED_SEGMENTS_SINKS
              ),
              schema,
              tuningConfig,
              dataSegmentPusher,
              rowIngestionMeters,
              new ParseExceptionHandler(
                  rowIngestionMeters,
                  false,
                  Integer.MAX_VALUE,
                  0
              ),
              false
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

    public DataSchema getSchema()
    {
      return schema;
    }

    public AppenderatorConfig getTuningConfig()
    {
      return tuningConfig;
    }

    public FireDepartmentMetrics getMetrics()
    {
      return metrics;
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
      emitter.close();
      FileUtils.deleteDirectory(tuningConfig.getBasePersistDirectory());
    }

    private static File createNewBasePersistDirectory()
    {
      return FileUtils.createTempDir("druid-batch-persist");
    }


    static class TestIndexTuningConfig implements AppenderatorConfig
    {
      private final AppendableIndexSpec appendableIndexSpec;
      private final int maxRowsInMemory;
      private final long maxBytesInMemory;
      private final boolean skipBytesInMemoryOverheadCheck;
      private final int maxColumnsToMerge;
      private final PartitionsSpec partitionsSpec;
      private final IndexSpec indexSpec;
      private final File basePersistDirectory;
      private final int maxPendingPersists;
      private final boolean reportParseExceptions;
      private final long pushTimeout;
      private final IndexSpec indexSpecForIntermediatePersists;
      @Nullable
      private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
      private final int numPersistThreads;

      public TestIndexTuningConfig(
          AppendableIndexSpec appendableIndexSpec,
          Integer maxRowsInMemory,
          Long maxBytesInMemory,
          Boolean skipBytesInMemoryOverheadCheck,
          IndexSpec indexSpec,
          Integer maxPendingPersists,
          Boolean reportParseExceptions,
          Long pushTimeout,
          @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
          Integer maxColumnsToMerge,
          File basePersistDirectory,
          Integer numPersistThreads
      )
      {
        this.appendableIndexSpec = appendableIndexSpec;
        this.maxRowsInMemory = maxRowsInMemory;
        this.maxBytesInMemory = maxBytesInMemory;
        this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
        this.indexSpec = indexSpec;
        this.maxPendingPersists = maxPendingPersists;
        this.reportParseExceptions = reportParseExceptions;
        this.pushTimeout = pushTimeout;
        this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
        this.maxColumnsToMerge = maxColumnsToMerge;
        this.basePersistDirectory = basePersistDirectory;

        this.partitionsSpec = null;
        this.indexSpecForIntermediatePersists = this.indexSpec;

        this.numPersistThreads = numPersistThreads == null ? DEFAULT_NUM_PERSIST_THREADS : numPersistThreads;
      }

      @Override
      public TestIndexTuningConfig withBasePersistDirectory(File dir)
      {
        return this;
      }

      @Override
      public AppendableIndexSpec getAppendableIndexSpec()
      {
        return appendableIndexSpec;
      }

      @Override
      public int getMaxRowsInMemory()
      {
        return maxRowsInMemory;
      }

      @Override
      public long getMaxBytesInMemory()
      {
        return maxBytesInMemory;
      }

      @Override
      public boolean isSkipBytesInMemoryOverheadCheck()
      {
        return skipBytesInMemoryOverheadCheck;
      }

      @Nullable
      @Override
      public PartitionsSpec getPartitionsSpec()
      {
        return partitionsSpec;
      }

      @Override
      public IndexSpec getIndexSpec()
      {
        return indexSpec;
      }

      @Override
      public IndexSpec getIndexSpecForIntermediatePersists()
      {
        return indexSpecForIntermediatePersists;
      }

      @Override
      public int getMaxPendingPersists()
      {
        return maxPendingPersists;
      }

      @Override
      public boolean isReportParseExceptions()
      {
        return reportParseExceptions;
      }

      @Nullable
      @Override
      public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
      {
        return segmentWriteOutMediumFactory;
      }

      @Override
      public int getMaxColumnsToMerge()
      {
        return maxColumnsToMerge;
      }

      @Override
      public File getBasePersistDirectory()
      {
        return basePersistDirectory;
      }

      @Override
      public Period getIntermediatePersistPeriod()
      {
        return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
      }

      @Override
      public int getNumPersistThreads()
      {
        return numPersistThreads;
      }

      @Override
      public boolean equals(Object o)
      {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        TestIndexTuningConfig that = (TestIndexTuningConfig) o;
        return Objects.equals(appendableIndexSpec, that.appendableIndexSpec) &&
               maxRowsInMemory == that.maxRowsInMemory &&
               maxBytesInMemory == that.maxBytesInMemory &&
               skipBytesInMemoryOverheadCheck == that.skipBytesInMemoryOverheadCheck &&
               maxColumnsToMerge == that.maxColumnsToMerge &&
               maxPendingPersists == that.maxPendingPersists &&
               reportParseExceptions == that.reportParseExceptions &&
               pushTimeout == that.pushTimeout &&
               numPersistThreads == that.numPersistThreads &&
               Objects.equals(partitionsSpec, that.partitionsSpec) &&
               Objects.equals(indexSpec, that.indexSpec) &&
               Objects.equals(indexSpecForIntermediatePersists, that.indexSpecForIntermediatePersists) &&
               Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
               Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory);
      }

      @Override
      public int hashCode()
      {
        return Objects.hash(
            appendableIndexSpec,
            maxRowsInMemory,
            maxBytesInMemory,
            skipBytesInMemoryOverheadCheck,
            maxColumnsToMerge,
            partitionsSpec,
            indexSpec,
            indexSpecForIntermediatePersists,
            basePersistDirectory,
            maxPendingPersists,
            reportParseExceptions,
            pushTimeout,
            segmentWriteOutMediumFactory,
            numPersistThreads
        );
      }

      @Override
      public String toString()
      {
        return "IndexTuningConfig{" +
               "maxRowsInMemory=" + maxRowsInMemory +
               ", maxBytesInMemory=" + maxBytesInMemory +
               ", skipBytesInMemoryOverheadCheck=" + skipBytesInMemoryOverheadCheck +
               ", maxColumnsToMerge=" + maxColumnsToMerge +
               ", partitionsSpec=" + partitionsSpec +
               ", indexSpec=" + indexSpec +
               ", indexSpecForIntermediatePersists=" + indexSpecForIntermediatePersists +
               ", basePersistDirectory=" + basePersistDirectory +
               ", maxPendingPersists=" + maxPendingPersists +
               ", reportParseExceptions=" + reportParseExceptions +
               ", pushTimeout=" + pushTimeout +
               ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
               ", numPersistThreads=" + numPersistThreads +
               '}';
      }
    }

    private static TaskToolbox makeTaskToolbox(
        ObjectMapper mapper,
        IndexMergerV9 indexMergerV9,
        TaskConfig.BatchProcessingMode mode
    )
    {
      TaskConfig config = new TaskConfigBuilder()
          .setBatchProcessingMode(mode.name())
          .build();
      return new TaskToolbox.Builder()
          .config(config)
          .joinableFactory(NoopJoinableFactory.INSTANCE)
          .jsonMapper(mapper)
          .indexIO(new IndexIO(new ObjectMapper(), ColumnConfig.DEFAULT))
          .indexMergerV9(indexMergerV9)
          .taskReportFileWriter(new NoopTestTaskReportFileWriter())
          .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
          .chatHandlerProvider(new NoopChatHandlerProvider())
          .appenderatorsManager(new TestAppenderatorsManager())
          .taskLogPusher(null)
          .attemptId("1")
          .build();

    }
  }
}

