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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.joda.time.Interval;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
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
 * @param <T> Type of task created by this builder.
 * @param <C> Type of tuning config used by this builder.
 * @see #ofTypeIndex()
 * @see #tuningConfig(Consumer) to specify the {@code tuningConfig}.
 */
@SuppressWarnings("unchecked")
public abstract class TaskBuilder<B extends TaskBuilder<B, C, T>, C, T extends Task>
{
  // Fields are package-protected to allow access by subclasses like Index, Compact
  InputSource inputSource = null;
  InputFormat inputFormat = null;

  final Map<String, Object> context = new HashMap<>();

  Boolean appendToExisting = null;

  final TuningConfigBuilder<C> tuningConfig;

  private TaskBuilder()
  {
    this.tuningConfig = tuningConfigBuilder();
  }

  /**
   * Creates a raw Map-based payload for a {@code Task} that may be submitted to
   * the Overlord using {@code OverlordClient.runTask()}.
   */
  public abstract T withId(String taskId);

  abstract TuningConfigBuilder<C> tuningConfigBuilder();

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

  public B inputSource(InputSource inputSource)
  {
    this.inputSource = inputSource;
    return (B) this;
  }

  public B inlineInputSourceWithData(String data)
  {
    return inputSource(new InlineInputSource(data));
  }

  public B druidInputSource(String dataSource, Interval interval)
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
  public B localInputSourceWithFiles(String... resources)
  {
    try {
      final List<File> files = new ArrayList<>();
      for (String file : resources) {
        final URL resourceUrl = getClass().getClassLoader().getResource(file);
        if (resourceUrl == null) {
          throw new ISE("Could not find file[%s]", file);
        }

        files.add(new File(resourceUrl.toURI()));
      }

      return inputSource(
          new LocalInputSource(null, null, files, null)
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public B inputFormat(InputFormat inputFormat)
  {
    this.inputFormat = inputFormat;
    return (B) this;
  }

  public B jsonInputFormat()
  {
    return inputFormat(
        new JsonInputFormat(null, null, null, null, null)
    );
  }

  public B csvInputFormatWithColumns(String... columns)
  {
    return inputFormat(
        new CsvInputFormat(List.of(columns), null, null, false, 0, null)
    );
  }

  public B appendToExisting(boolean append)
  {
    this.appendToExisting = append;
    return (B) this;
  }

  public B dynamicPartitionWithMaxRows(int maxRowsPerSegment)
  {
    tuningConfig.withPartitionsSpec(new DynamicPartitionsSpec(maxRowsPerSegment, null));
    return (B) this;
  }

  public B tuningConfig(Consumer<TuningConfigBuilder<C>> updateTuningConfig)
  {
    updateTuningConfig.accept(tuningConfig);
    return (B) this;
  }

  public B context(String key, Object value)
  {
    this.context.put(key, value);
    return (B) this;
  }

  public abstract static class IndexCommon<B extends TaskBuilder<B, C, T>, C, T extends Task>
      extends TaskBuilder<B, C, T>
  {
    final DataSchema.Builder dataSchema = DataSchema.builder();

    public B dataSource(String dataSource)
    {
      dataSchema.withDataSource(dataSource);
      return (B) this;
    }

    public B dataSchema(Consumer<DataSchema.Builder> updateDataSchema)
    {
      updateDataSchema.accept(dataSchema);
      return (B) this;
    }

    public B isoTimestampColumn(String timestampColumn)
    {
      dataSchema.withTimestamp(new TimestampSpec(timestampColumn, "iso", null));
      return (B) this;
    }

    public B timestampColumn(String timestampColumn)
    {
      dataSchema.withTimestamp(new TimestampSpec(timestampColumn, null, null));
      return (B) this;
    }

    public B granularitySpec(GranularitySpec granularitySpec)
    {
      dataSchema.withGranularity(granularitySpec);
      return (B) this;
    }

    public B granularitySpec(String segmentGranularity, String queryGranularity, Boolean rollup)
    {
      dataSchema.withGranularity(
          new UniformGranularitySpec(
              Granularity.fromString(segmentGranularity),
              queryGranularity == null ? null : Granularity.fromString(queryGranularity),
              rollup,
              null
          )
      );
      return (B) this;
    }

    /**
     * Sets {@code "granularitySpec": {"segmentGranularity": <arg>}}.
     */
    public B segmentGranularity(String granularity)
    {
      return granularitySpec(granularity, null, null);
    }

    /**
     * Sets the given dimensions as string dimensions in the {@link DataSchema}.
     *
     * @see #dataSchema(Consumer) for more options
     */
    public B dimensions(String... dimensions)
    {
      dataSchema.withDimensions(
          Stream.of(dimensions)
                .map(StringDimensionSchema::new)
                .collect(Collectors.toList())
      );
      return (B) this;
    }

    public B metricAggregates(AggregatorFactory... aggregators)
    {
      dataSchema.withAggregators(aggregators);
      return (B) this;
    }
  }

  /**
   * Builder for {@link IndexTask} that uses a {@link IndexTask.IndexTuningConfig}.
   */
  public static class Index extends IndexCommon<Index, IndexTask.IndexTuningConfig, IndexTask>
  {
    @Override
    public TuningConfigBuilder<IndexTask.IndexTuningConfig> tuningConfigBuilder()
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
  public static class IndexParallel extends IndexCommon<IndexParallel, ParallelIndexTuningConfig, ParallelIndexSupervisorTask>
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
    public TuningConfigBuilder<ParallelIndexTuningConfig> tuningConfigBuilder()
    {
      return TuningConfigBuilder.forParallelIndexTask();
    }
  }

  /**
   * Builder for a {@link CompactionTask} which uses a {@link CompactionTask.CompactionTuningConfig}.
   */
  public static class Compact extends TaskBuilder<Compact, CompactionTask.CompactionTuningConfig, CompactionTask>
  {
    private String dataSource;
    private Interval interval;
    private DimensionsSpec dimensionsSpec;

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

    @Override
    public CompactionTask withId(String taskId)
    {
      return new CompactionTask(
          taskId,
          null,
          dataSource,
          interval,
          null,
          null,
          dimensionsSpec,
          null,
          null,
          null,
          null,
          null,
          null,
          tuningConfig.build(),
          null,
          null,
          null
      );
    }

    @Override
    public TuningConfigBuilder<CompactionTask.CompactionTuningConfig> tuningConfigBuilder()
    {
      return TuningConfigBuilder.forCompactionTask();
    }
  }
}
