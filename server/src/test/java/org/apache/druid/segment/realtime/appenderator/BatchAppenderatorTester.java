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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
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
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class BatchAppenderatorTester implements AutoCloseable
{
  public static final String DATASOURCE = "foo";

  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final FireDepartmentMetrics metrics;
  private final ObjectMapper objectMapper;
  private final Appenderator appenderator;
  private final ServiceEmitter emitter;

  private final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<>();

  public BatchAppenderatorTester(
      final int maxRowsInMemory
  )
  {
    this(maxRowsInMemory, -1, null, false);
  }

  public BatchAppenderatorTester(
      final int maxRowsInMemory,
      final boolean enablePushFailure
  )
  {
    this(maxRowsInMemory, -1, null, enablePushFailure);
  }

  public BatchAppenderatorTester(
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      final boolean enablePushFailure
  )
  {
    this(maxRowsInMemory, maxSizeInBytes, null, enablePushFailure);
  }

  public BatchAppenderatorTester(
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      final File basePersistDirectory,
      final boolean enablePushFailure
  )
  {
    this(
        maxRowsInMemory,
        maxSizeInBytes,
        basePersistDirectory,
        enablePushFailure,
        new SimpleRowIngestionMeters(),
        false,
        false
    );
  }

  public BatchAppenderatorTester(
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      @Nullable final File basePersistDirectory,
      final boolean enablePushFailure,
      final RowIngestionMeters rowIngestionMeters
  )
  {
    this(maxRowsInMemory, maxSizeInBytes, basePersistDirectory, enablePushFailure, rowIngestionMeters,
         false, false
    );
  }
  
  public BatchAppenderatorTester(
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      @Nullable final File basePersistDirectory,
      final boolean enablePushFailure,
      final RowIngestionMeters rowIngestionMeters,
      final boolean skipBytesInMemoryOverheadCheck,
      final boolean useLegacyBatchProcessing
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
        new IndexSpec(),
        0,
        false,
        false,
        0L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        false,
        0,
        1,
        IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE,
        0L,
        basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory
    );
    metrics = new FireDepartmentMetrics();

    IndexIO indexIO = new IndexIO(
        objectMapper,
        () -> 0
    );
    IndexMerger indexMerger = new IndexMergerV9(
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
    appenderator = Appenderators.createOffline(
        schema.getDataSource(),
        schema,
        tuningConfig,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger,
        rowIngestionMeters,
        new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
        useLegacyBatchProcessing
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


  private static class TestIndexTuningConfig implements AppenderatorConfig
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
    private final boolean forceGuaranteedRollup;
    private final boolean reportParseExceptions;
    private final long pushTimeout;
    private final boolean logParseExceptions;
    private final int maxParseExceptions;
    private final int maxSavedParseExceptions;
    private final long awaitSegmentAvailabilityTimeoutMillis;
    private final IndexSpec indexSpecForIntermediatePersists;
    @Nullable
    private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

    public TestIndexTuningConfig(
         AppendableIndexSpec appendableIndexSpec,
         Integer maxRowsInMemory,
         Long maxBytesInMemory,
         Boolean skipBytesInMemoryOverheadCheck,
         IndexSpec indexSpec,
         Integer maxPendingPersists,
         Boolean forceGuaranteedRollup,
         Boolean reportParseExceptions,
         Long pushTimeout,
         @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
         Boolean logParseExceptions,
         Integer maxParseExceptions,
         Integer maxSavedParseExceptions,
         Integer maxColumnsToMerge,
         Long awaitSegmentAvailabilityTimeoutMillis,
         File basePersistDirectory
    )
    {
      this.appendableIndexSpec = appendableIndexSpec;
      this.maxRowsInMemory = maxRowsInMemory;
      this.maxBytesInMemory = maxBytesInMemory;
      this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
      this.indexSpec = indexSpec;
      this.maxPendingPersists = maxPendingPersists;
      this.forceGuaranteedRollup = forceGuaranteedRollup;
      this.reportParseExceptions = reportParseExceptions;
      this.pushTimeout = pushTimeout;
      this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
      this.logParseExceptions = logParseExceptions;
      this.maxParseExceptions = maxParseExceptions;
      this.maxSavedParseExceptions = Math.min(1, maxSavedParseExceptions);
      this.maxColumnsToMerge = maxColumnsToMerge;
      this.awaitSegmentAvailabilityTimeoutMillis = awaitSegmentAvailabilityTimeoutMillis;
      this.basePersistDirectory = basePersistDirectory;

      this.partitionsSpec = null;
      this.indexSpecForIntermediatePersists = this.indexSpec;
    }

    @Override
    public TestIndexTuningConfig withBasePersistDirectory(File dir)
    {
      throw new UnsupportedOperationException();
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

    public PartitionsSpec getGivenOrDefaultPartitionsSpec()
    {
      if (partitionsSpec != null) {
        return partitionsSpec;
      }
      return forceGuaranteedRollup
             ? new HashedPartitionsSpec(null, null, null)
             : new DynamicPartitionsSpec(null, null);
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

    public boolean isForceGuaranteedRollup()
    {
      return forceGuaranteedRollup;
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

    public boolean isLogParseExceptions()
    {
      return logParseExceptions;
    }

    public int getMaxParseExceptions()
    {
      return maxParseExceptions;
    }

    public int getMaxSavedParseExceptions()
    {
      return maxSavedParseExceptions;
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
    
    public long getAwaitSegmentAvailabilityTimeoutMillis()
    {
      return awaitSegmentAvailabilityTimeoutMillis;
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
             forceGuaranteedRollup == that.forceGuaranteedRollup &&
             reportParseExceptions == that.reportParseExceptions &&
             pushTimeout == that.pushTimeout &&
             logParseExceptions == that.logParseExceptions &&
             maxParseExceptions == that.maxParseExceptions &&
             maxSavedParseExceptions == that.maxSavedParseExceptions &&
             Objects.equals(partitionsSpec, that.partitionsSpec) &&
             Objects.equals(indexSpec, that.indexSpec) &&
             Objects.equals(indexSpecForIntermediatePersists, that.indexSpecForIntermediatePersists) &&
             Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
             Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
             Objects.equals(awaitSegmentAvailabilityTimeoutMillis, that.awaitSegmentAvailabilityTimeoutMillis);
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
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          segmentWriteOutMediumFactory,
          awaitSegmentAvailabilityTimeoutMillis
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
             ", forceGuaranteedRollup=" + forceGuaranteedRollup +
             ", reportParseExceptions=" + reportParseExceptions +
             ", pushTimeout=" + pushTimeout +
             ", logParseExceptions=" + logParseExceptions +
             ", maxParseExceptions=" + maxParseExceptions +
             ", maxSavedParseExceptions=" + maxSavedParseExceptions +
             ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
             ", awaitSegmentAvailabilityTimeoutMillis=" + awaitSegmentAvailabilityTimeoutMillis +
             '}';
    }
  }

}
