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

import com.google.common.base.Preconditions;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.joda.time.Interval;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builder for the {@link Task} objects.
 * <p>
 * The builder does not use any defaults and all required fields must be set
 * explicitly.
 *
 * @param <Self> Type of this builder itself
 * @param <C> Type of tuning config used by this builder.
 * @param <T> Type of task created by this builder.
 * @param <CB> Type of tuning config builder
 * @see #ofTypeIndex()
 * @see #tuningConfig(Consumer) to specify the {@code tuningConfig}.
 */
@SuppressWarnings("unchecked")
public abstract class TaskBuilder<
    T extends Task,
    C,
    CB extends TuningConfigBuilder<CB, C>,
    Self extends TaskBuilder<T, C, CB, Self>>
{
  // Fields are package-protected to allow access by subclasses like Index, Compact
  InputSource inputSource = null;
  InputFormat inputFormat = null;

  final Map<String, Object> context = new HashMap<>();

  Boolean appendToExisting = null;

  final TuningConfigBuilder<CB, C> tuningConfig;

  private TaskBuilder()
  {
    this.tuningConfig = tuningConfigBuilder();
  }

  /**
   * Creates a raw Map-based payload for a {@code Task} that may be submitted to
   * the Overlord using {@code OverlordClient.runTask()}.
   */
  public abstract T withId(String taskId);

  public abstract Self dataSource(String dataSource);

  protected abstract TuningConfigBuilder<CB, C> tuningConfigBuilder();

  /**
   * Initializes builder for a new {@link IndexTask}.
   */
  public static Index ofTypeIndex()
  {
    return new Index();
  }

  /**
   * Initializes builder for a new {@link ParallelIndexSupervisorTask}.
   */
  public static IndexParallel ofTypeIndexParallel()
  {
    return new IndexParallel();
  }

  public static Compact ofTypeCompact()
  {
    return new Compact();
  }

  public Self inputSource(InputSource inputSource)
  {
    this.inputSource = inputSource;
    return (Self) this;
  }

  public Self inlineInputSourceWithData(String data)
  {
    return inputSource(new InlineInputSource(data));
  }

  public Self druidInputSource(String dataSource, Interval interval)
  {
    return inputSource(
        new DruidInputSource(
            dataSource,
            interval,
            null,
            null,
            null,
            null,
            TestHelper.getTestIndexIO(),
            new NoopCoordinatorClient(),
            new SegmentCacheManagerFactory(TestHelper.getTestIndexIO(), TestHelper.JSON_MAPPER),
            new TaskConfigBuilder().build()
        )
    );
  }

  /**
   * Gets the absolute path of the given resource files and sets:
   * <pre>
   * "inputSource": {
   *   "type": "local",
   *   "files": [&lt;absolute-paths-of-given-resource-files&gt;]
   * }
   * </pre>
   */
  public Self localInputSourceWithFiles(File... files)
  {
    return inputSource(
        new LocalInputSource(null, null, List.of(files), null)
    );
  }

  public Self inputFormat(InputFormat inputFormat)
  {
    this.inputFormat = inputFormat;
    return (Self) this;
  }

  public Self jsonInputFormat()
  {
    return inputFormat(
        new JsonInputFormat(null, null, null, null, null)
    );
  }

  public Self csvInputFormatWithColumns(String... columns)
  {
    return inputFormat(
        new CsvInputFormat(List.of(columns), null, null, false, 0, null)
    );
  }

  public Self appendToExisting(boolean append)
  {
    this.appendToExisting = append;
    return (Self) this;
  }

  public Self dynamicPartitionWithMaxRows(int maxRowsPerSegment)
  {
    tuningConfig.withPartitionsSpec(new DynamicPartitionsSpec(maxRowsPerSegment, null));
    return (Self) this;
  }

  public Self tuningConfig(Consumer<TuningConfigBuilder<CB, C>> updateTuningConfig)
  {
    updateTuningConfig.accept(tuningConfig);
    return (Self) this;
  }

  public Self context(String key, Object value)
  {
    this.context.put(key, value);
    return (Self) this;
  }

  public abstract static class IndexCommon<
      T extends Task,
      C,
      CB extends TuningConfigBuilder<CB, C>,
      Self extends TaskBuilder<T, C, CB, Self>>
      extends TaskBuilder<T, C, CB, Self>
  {
    final DataSchema.Builder dataSchema = DataSchema.builder();

    @Override
    public Self dataSource(String dataSource)
    {
      dataSchema.withDataSource(dataSource);
      return (Self) this;
    }

    public Self dataSchema(Consumer<DataSchema.Builder> updateDataSchema)
    {
      updateDataSchema.accept(dataSchema);
      return (Self) this;
    }

    public Self isoTimestampColumn(String timestampColumn)
    {
      dataSchema.withTimestamp(new TimestampSpec(timestampColumn, "iso", null));
      return (Self) this;
    }

    public Self timestampColumn(String timestampColumn)
    {
      dataSchema.withTimestamp(new TimestampSpec(timestampColumn, null, null));
      return (Self) this;
    }

    public Self granularitySpec(GranularitySpec granularitySpec)
    {
      dataSchema.withGranularity(granularitySpec);
      return (Self) this;
    }

    public Self granularitySpec(String segmentGranularity, String queryGranularity, Boolean rollup)
    {
      dataSchema.withGranularity(
          new UniformGranularitySpec(
              Granularity.fromString(segmentGranularity),
              queryGranularity == null ? null : Granularity.fromString(queryGranularity),
              rollup,
              null
          )
      );
      return (Self) this;
    }

    /**
     * Sets {@code "granularitySpec": {"segmentGranularity": <arg>}}.
     */
    public Self segmentGranularity(String granularity)
    {
      return granularitySpec(granularity, null, null);
    }

    /**
     * Sets the given dimensions as string dimensions in the {@link DataSchema}.
     *
     * @see #dataSchema(Consumer) for more options
     */
    public Self dimensions(String... dimensions)
    {
      dataSchema.withDimensions(
          Stream.of(dimensions)
                .map(StringDimensionSchema::new)
                .collect(Collectors.toList())
      );
      return (Self) this;
    }

    public Self metricAggregates(AggregatorFactory... aggregators)
    {
      dataSchema.withAggregators(aggregators);
      return (Self) this;
    }
  }

  /**
   * Builder for {@link IndexTask} that uses a {@link IndexTask.IndexTuningConfig}.
   */
  public static class Index extends IndexCommon<
      IndexTask,
      IndexTask.IndexTuningConfig,
      TuningConfigBuilder.Index,
      Index>
  {
    @Override
    public TuningConfigBuilder.Index tuningConfigBuilder()
    {
      return TuningConfigBuilder.forIndexTask();
    }

    @Override
    public IndexTask withId(String taskId)
    {
      Preconditions.checkNotNull(inputSource, "'inputSource' must be specified");
      
      return new IndexTask(
          taskId,
          null,
          new IndexTask.IndexIngestionSpec(
              dataSchema.build(),
              new IndexTask.IndexIOConfig(
                  inputSource,
                  inputFormat,
                  appendToExisting,
                  null
              ),
              tuningConfig.build()
          ),
          context
      );
    }
  }

  /**
   * Builder for {@link ParallelIndexSupervisorTask} which uses a {@link ParallelIndexTuningConfig}.
   */
  public static class IndexParallel extends IndexCommon<
      ParallelIndexSupervisorTask,
      ParallelIndexTuningConfig,
      TuningConfigBuilder.ParallelIndex,
      IndexParallel>
  {
    @Override
    public ParallelIndexSupervisorTask withId(String taskId)
    {
      Preconditions.checkNotNull(inputSource, "'inputSource' must be specified");
      return new ParallelIndexSupervisorTask(
          taskId,
          null,
          null,
          new ParallelIndexIngestionSpec(
              dataSchema.build(),
              new ParallelIndexIOConfig(
                  inputSource,
                  inputFormat,
                  appendToExisting,
                  null
              ),
              tuningConfig.build()
          ),
          context
      );
    }

    @Override
    public TuningConfigBuilder.ParallelIndex tuningConfigBuilder()
    {
      return TuningConfigBuilder.forParallelIndexTask();
    }
  }

  /**
   * Builder for a {@link CompactionTask} which uses a {@link CompactionTask.CompactionTuningConfig}.
   */
  public static class Compact extends TaskBuilder<
      CompactionTask,
      CompactionTask.CompactionTuningConfig,
      TuningConfigBuilder.Compact,
      Compact>
  {
    private String dataSource;
    private Interval interval;
    private DimensionsSpec dimensionsSpec;
    private Granularity segmentGranularity;
    private ClientCompactionTaskGranularitySpec granularitySpec;
    private CompactionIOConfig ioConfig;

    @Override
    public Compact dataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Compact interval(Interval interval)
    {
      this.interval = interval;
      return this;
    }

    public Compact dimensions(String... dimensions)
    {
      dimensionsSpec = new DimensionsSpec(
          Stream.of(dimensions)
                .map(StringDimensionSchema::new)
                .collect(Collectors.toList())
      );
      return this;
    }

    public Compact granularitySpec(ClientCompactionTaskGranularitySpec granularitySpec)
    {
      this.granularitySpec = granularitySpec;
      return this;
    }

    public Compact segmentGranularity(Granularity segmentGranularity)
    {
      this.segmentGranularity = segmentGranularity;
      return this;
    }

    public Compact ioConfig(CompactionInputSpec inputSpec, boolean allowNonAlignedInterval)
    {
      this.ioConfig = new CompactionIOConfig(inputSpec, allowNonAlignedInterval, null);
      return this;
    }

    @Override
    public CompactionTask withId(String taskId)
    {
      return new CompactionTask(
          taskId,
          null,
          dataSource,
          interval,
          null,
          ioConfig,
          dimensionsSpec,
          null,
          null,
          null,
          segmentGranularity,
          granularitySpec,
          null,
          tuningConfig.build(),
          null,
          null,
          null
      );
    }

    @Override
    public TuningConfigBuilder.Compact tuningConfigBuilder()
    {
      return TuningConfigBuilder.forCompactionTask();
    }
  }
}
