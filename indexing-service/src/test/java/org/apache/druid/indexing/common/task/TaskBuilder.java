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
 * @see #dataSchema(Consumer) to specify the {@code dataSchema}.
 * @see #tuningConfig(Consumer) to specify the {@code tuningConfig}.
 */
public class TaskBuilder<T extends Task, C>
{
  private InputSource inputSource = null;
  private InputFormat inputFormat = null;

  private final Map<String, Object> context = new HashMap<>();

  private Boolean appendToExisting = null;

  private final BuilderType<T, C> type;
  private final DataSchema.Builder dataSchema;
  private final TuningConfigBuilder<C> tuningConfig;

  private TaskBuilder(BuilderType<T, C> type)
  {
    this.type = type;
    this.dataSchema = DataSchema.builder();
    this.tuningConfig = type.tuningConfigBuilder();
  }

  /**
   * Initializes builder for a new {@link IndexTask}.
   */
  public static TaskBuilder<IndexTask, IndexTask.IndexTuningConfig> ofTypeIndex()
  {
    return new TaskBuilder<>(new Index());
  }

  /**
   * Initializes builder for a new {@link ParallelIndexSupervisorTask}.
   */
  public static TaskBuilder<ParallelIndexSupervisorTask, ParallelIndexTuningConfig> ofTypeIndexParallel()
  {
    return new TaskBuilder<>(new ParallelIndex());
  }

  public TaskBuilder<T, C> dataSource(String dataSource)
  {
    dataSchema.withDataSource(dataSource);
    return this;
  }

  /**
   * Creates a raw Map-based payload for a {@code Task} that may be submitted to
   * the Overlord using {@code OverlordClient.runTask()}.
   */
  public T withId(String taskId)
  {
    Preconditions.checkNotNull(taskId, "Task ID must not be null");
    Preconditions.checkNotNull(type, "Task type must be specified");
    Preconditions.checkNotNull(inputSource, "'inputSource' must be specified");

    return type.buildTask(taskId, this);
  }

  public TaskBuilder<T, C> inputSource(InputSource inputSource)
  {
    this.inputSource = inputSource;
    return this;
  }

  public TaskBuilder<T, C> inlineInputSourceWithData(String data)
  {
    return inputSource(new InlineInputSource(data));
  }

  public TaskBuilder<T, C> druidInputSource(String dataSource, Interval interval)
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
  public TaskBuilder<T, C> localInputSourceWithFiles(String... resources)
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

  public TaskBuilder<T, C> inputFormat(InputFormat inputFormat)
  {
    this.inputFormat = inputFormat;
    return this;
  }

  public TaskBuilder<T, C> jsonInputFormat()
  {
    return inputFormat(
        new JsonInputFormat(null, null, null, null, null)
    );
  }

  public TaskBuilder<T, C> csvInputFormatWithColumns(String... columns)
  {
    return inputFormat(
        new CsvInputFormat(List.of(columns), null, null, false, 0, null)
    );
  }

  public TaskBuilder<T, C> appendToExisting(boolean append)
  {
    this.appendToExisting = append;
    return this;
  }

  public TaskBuilder<T, C> dynamicPartitionWithMaxRows(int maxRowsPerSegment)
  {
    tuningConfig.withPartitionsSpec(new DynamicPartitionsSpec(maxRowsPerSegment, null));
    return this;
  }

  public TaskBuilder<T, C> granularitySpec(GranularitySpec granularitySpec)
  {
    dataSchema.withGranularity(granularitySpec);
    return this;
  }

  public TaskBuilder<T, C> granularitySpec(String segmentGranularity, String queryGranularity, Boolean rollup)
  {
    dataSchema.withGranularity(
        new UniformGranularitySpec(
            Granularity.fromString(segmentGranularity),
            queryGranularity == null ? null : Granularity.fromString(queryGranularity),
            rollup,
            null
        )
    );
    return this;
  }

  /**
   * Sets {@code "granularitySpec": {"segmentGranularity": <arg>}}.
   */
  public TaskBuilder<T, C> segmentGranularity(String granularity)
  {
    return granularitySpec(granularity, null, null);
  }

  public TaskBuilder<T, C> isoTimestampColumn(String timestampColumn)
  {
    dataSchema.withTimestamp(new TimestampSpec(timestampColumn, "iso", null));
    return this;
  }

  public TaskBuilder<T, C> timestampColumn(String timestampColumn)
  {
    dataSchema.withTimestamp(new TimestampSpec(timestampColumn, null, null));
    return this;
  }

  /**
   * Sets the given dimensions as string dimensions in the {@link DataSchema}.
   *
   * @see #dataSchema(Consumer) for more options
   */
  public TaskBuilder<T, C> dimensions(String... dimensions)
  {
    dataSchema.withDimensions(
        Stream.of(dimensions).map(StringDimensionSchema::new).collect(Collectors.toList())
    );
    return this;
  }

  public TaskBuilder<T, C> metricAggregates(AggregatorFactory... aggregators)
  {
    dataSchema.withAggregators(aggregators);
    return this;
  }

  public TaskBuilder<T, C> tuningConfig(Consumer<TuningConfigBuilder<C>> updateTuningConfig)
  {
    updateTuningConfig.accept(tuningConfig);
    return this;
  }

  public TaskBuilder<T, C> dataSchema(Consumer<DataSchema.Builder> updateDataSchema)
  {
    updateDataSchema.accept(dataSchema);
    return this;
  }

  public TaskBuilder<T, C> context(String key, Object value)
  {
    this.context.put(key, value);
    return this;
  }

  public interface BuilderType<T extends Task, C>
  {
    T buildTask(String taskId, TaskBuilder<T, C> builder);

    TuningConfigBuilder<C> tuningConfigBuilder();
  }

  private static class Index implements BuilderType<IndexTask, IndexTask.IndexTuningConfig>
  {
    @Override
    public TuningConfigBuilder<IndexTask.IndexTuningConfig> tuningConfigBuilder()
    {
      return TuningConfigBuilder.forIndexTask();
    }

    @Override
    public IndexTask buildTask(String taskId, TaskBuilder<IndexTask, IndexTask.IndexTuningConfig> builder)
    {
      return new IndexTask(
          taskId,
          null,
          new IndexTask.IndexIngestionSpec(
              builder.dataSchema.build(),
              new IndexTask.IndexIOConfig(
                  builder.inputSource,
                  builder.inputFormat,
                  builder.appendToExisting,
                  null
              ),
              builder.tuningConfig.build()
          ),
          builder.context
      );
    }
  }

  private static class ParallelIndex implements BuilderType<ParallelIndexSupervisorTask, ParallelIndexTuningConfig>
  {

    @Override
    public ParallelIndexSupervisorTask buildTask(
        String taskId,
        TaskBuilder<ParallelIndexSupervisorTask, ParallelIndexTuningConfig> builder
    )
    {
      return new ParallelIndexSupervisorTask(
          taskId,
          null,
          null,
          new ParallelIndexIngestionSpec(
              builder.dataSchema.build(),
              new ParallelIndexIOConfig(
                  builder.inputSource,
                  builder.inputFormat,
                  builder.appendToExisting,
                  null
              ),
              builder.tuningConfig.build()
          ),
          builder.context
      );
    }

    @Override
    public TuningConfigBuilder<ParallelIndexTuningConfig> tuningConfigBuilder()
    {
      return TuningConfigBuilder.forParallelIndexTask();
    }
  }
}
