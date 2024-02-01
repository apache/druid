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

package org.apache.druid.msq.indexing.processor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.counters.SegmentGenerationProgressCounter;
import org.apache.druid.msq.counters.SegmentGeneratorMetricsWrapper;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.ExtraInfoHolder;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

@JsonTypeName("segmentGenerator")
public class SegmentGeneratorFrameProcessorFactory
    implements FrameProcessorFactory<DataSegment, Set<DataSegment>, List<SegmentIdWithShardSpec>>
{
  private final DataSchema dataSchema;
  private final ColumnMappings columnMappings;
  private final MSQTuningConfig tuningConfig;

  @JsonCreator
  public SegmentGeneratorFrameProcessorFactory(
      @JsonProperty("dataSchema") final DataSchema dataSchema,
      @JsonProperty("columnMappings") final ColumnMappings columnMappings,
      @JsonProperty("tuningConfig") final MSQTuningConfig tuningConfig
  )
  {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public ColumnMappings getColumnMappings()
  {
    return columnMappings;
  }

  @JsonProperty
  public MSQTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public ProcessorsAndChannels<DataSegment, Set<DataSegment>> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable List<SegmentIdWithShardSpec> extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  )
  {
    if (extra == null || extra.isEmpty()) {
      return new ProcessorsAndChannels<>(
          ProcessorManagers.of(Sequences.<SegmentGeneratorFrameProcessor>empty())
                           .withAccumulation(new HashSet<>(), (acc, segment) -> acc),
          OutputChannels.none()
      );
    }

    final RowIngestionMeters meters = frameContext.rowIngestionMeters();

    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        meters,
        TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS,
        TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS,
        TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
    );

    // Expect a single input slice.
    final InputSlice slice = Iterables.getOnlyElement(inputSlices);
    final Sequence<Pair<Integer, ReadableInput>> inputSequence =
        Sequences.simple(Iterables.transform(
            inputSliceReader.attach(0, slice, counters, warningPublisher),
            new Function<ReadableInput, Pair<Integer, ReadableInput>>()
            {
              int i = 0;

              @Override
              public Pair<Integer, ReadableInput> apply(ReadableInput readableInput)
              {
                return Pair.of(i++, readableInput);
              }
            }
        ));
    final SegmentGenerationProgressCounter segmentGenerationProgressCounter = counters.segmentGenerationProgress();
    final SegmentGeneratorMetricsWrapper segmentGeneratorMetricsWrapper =
        new SegmentGeneratorMetricsWrapper(segmentGenerationProgressCounter);

    final Sequence<SegmentGeneratorFrameProcessor> workers = inputSequence.map(
        readableInputPair -> {
          final StagePartition stagePartition = Preconditions.checkNotNull(readableInputPair.rhs.getStagePartition());
          final SegmentIdWithShardSpec segmentIdWithShardSpec = extra.get(readableInputPair.lhs);
          final String idString = StringUtils.format("%s:%s", stagePartition, workerNumber);
          final File persistDirectory = new File(
              frameContext.persistDir(),
              segmentIdWithShardSpec.asSegmentId().toString()
          );

          // Create directly, without using AppenderatorsManager, because we need different memory overrides due to
          // using one Appenderator per processing thread instead of per task.
          // Note: "createOffline" ignores the batchProcessingMode and always acts like CLOSED_SEGMENTS_SINKS.
          final Appenderator appenderator =
              Appenderators.createOffline(
                  idString,
                  dataSchema,
                  makeAppenderatorConfig(
                      tuningConfig,
                      persistDirectory,
                      frameContext.memoryParameters()
                  ),
                  segmentGeneratorMetricsWrapper,
                  frameContext.segmentPusher(),
                  frameContext.jsonMapper(),
                  frameContext.indexIO(),
                  frameContext.indexMerger(),
                  meters,
                  parseExceptionHandler,
                  true
              );

          return new SegmentGeneratorFrameProcessor(
              readableInputPair.rhs,
              columnMappings,
              dataSchema.getDimensionsSpec().getDimensionNames(),
              appenderator,
              segmentIdWithShardSpec,
              segmentGenerationProgressCounter
          );
        }
    );

    return new ProcessorsAndChannels<>(
        ProcessorManagers.of(workers)
                         .withAccumulation(
                             new HashSet<>(),
                             (acc, segment) -> {
                               if (segment != null) {
                                 acc.add(segment);
                               }

                               return acc;
                             }
                         ),
        OutputChannels.none()
    );
  }

  @Override
  public TypeReference<Set<DataSegment>> getResultTypeReference()
  {
    return new TypeReference<Set<DataSegment>>() {};
  }

  @Nullable
  @Override
  public Set<DataSegment> mergeAccumulatedResult(Set<DataSegment> accumulated, Set<DataSegment> otherAccumulated)
  {
    accumulated.addAll(otherAccumulated);
    return accumulated;
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
    SegmentGeneratorFrameProcessorFactory that = (SegmentGeneratorFrameProcessorFactory) o;
    return Objects.equals(dataSchema, that.dataSchema)
           && Objects.equals(columnMappings, that.columnMappings)
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSchema, columnMappings, tuningConfig);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExtraInfoHolder makeExtraInfoHolder(final List<SegmentIdWithShardSpec> segmentIdsWithShardSpecs)
  {
    return new SegmentGeneratorExtraInfoHolder(segmentIdsWithShardSpecs);
  }

  private static AppenderatorConfig makeAppenderatorConfig(
      final MSQTuningConfig tuningConfig,
      final File persistDirectory,
      final WorkerMemoryParameters memoryParameters
  )
  {
    return new AppenderatorConfig()
    {
      @Override
      public AppendableIndexSpec getAppendableIndexSpec()
      {
        return TuningConfig.DEFAULT_APPENDABLE_INDEX;
      }

      @Override
      public int getMaxRowsInMemory()
      {
        // No need to apportion this amongst memoryParameters.getAppenderatorCount(), because it only exists
        // to minimize the impact of per-row overheads that are not accounted for by OnheapIncrementalIndex's
        // maxBytesInMemory handling. For example: overheads related to creating bitmaps during persist.
        return tuningConfig.getMaxRowsInMemory();
      }

      @Override
      public long getMaxBytesInMemory()
      {
        return memoryParameters.getAppenderatorMaxBytesInMemory();
      }

      @Override
      public PartitionsSpec getPartitionsSpec()
      {
        // We're not actually doing dynamic partitioning. This segment generator is going to get exactly one segment.
        return new DynamicPartitionsSpec(Integer.MAX_VALUE, Long.MAX_VALUE);
      }

      @Override
      public IndexSpec getIndexSpec()
      {
        return tuningConfig.getIndexSpec();
      }

      @Override
      public IndexSpec getIndexSpecForIntermediatePersists()
      {
        // Disable compression for intermediate persists to reduce direct memory usage.
        return IndexSpec.builder()
                        // Dimensions don't support NONE, so use UNCOMPRESSED
                        .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                        // NONE is more efficient than UNCOMPRESSED
                        .withMetricCompression(CompressionStrategy.NONE)
                        .withLongEncoding(CompressionFactory.LongEncodingStrategy.LONGS)
                        .build();
      }

      @Override
      public boolean isReportParseExceptions()
      {
        return true;
      }

      @Override
      public int getMaxPendingPersists()
      {
        return 0;
      }

      @Override
      public boolean isSkipBytesInMemoryOverheadCheck()
      {
        return TuningConfig.DEFAULT_SKIP_BYTES_IN_MEMORY_OVERHEAD_CHECK;
      }

      @Override
      public Period getIntermediatePersistPeriod()
      {
        // Intermediate persist doesn't make much sense for batch jobs.
        return new Period(Integer.MAX_VALUE);
      }

      @Override
      public File getBasePersistDirectory()
      {
        return persistDirectory;
      }

      @Override
      public AppenderatorConfig withBasePersistDirectory(final File basePersistDirectory)
      {
        // Not used.
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
      {
        // Default SegmentWriteOutMediumFactory.
        return null;
      }

      @Override
      public int getMaxColumnsToMerge()
      {
        return memoryParameters.getAppenderatorMaxColumnsToMerge();
      }
    };
  }

  @JsonTypeName("segmentGenerator")
  public static class SegmentGeneratorExtraInfoHolder extends ExtraInfoHolder<List<SegmentIdWithShardSpec>>
  {
    @JsonCreator
    public SegmentGeneratorExtraInfoHolder(@Nullable @JsonProperty(INFO_KEY) final List<SegmentIdWithShardSpec> extra)
    {
      super(extra);
    }
  }
}
