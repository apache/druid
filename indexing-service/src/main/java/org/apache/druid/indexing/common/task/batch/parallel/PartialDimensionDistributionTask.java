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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.hash.BloomFilter;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.HandlingInputRowIterator;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketch;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.TimeDimTuple;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.TimeDimTupleFactory;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.TimeDimTupleFunnel;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.IndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.RangePartitionIndexTaskInputRowIteratorBuilder;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The worker task of {@link PartialDimensionDistributionParallelIndexTaskRunner}. This task
 * determines the distribution of dimension values of input data.
 */

public class PartialDimensionDistributionTask extends PerfectRollupWorkerTask
{
  public static final String TYPE = "partial_dimension_distribution";
  private static final Logger LOG = new Logger(PartialDimensionDistributionTask.class);

  // Future work: StringDistribution does not handle inserting NULLs. This is the same behavior as hadoop indexing.
  private static final boolean SKIP_NULL = true;

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory;

  // For testing
  private final Supplier<DedupRowDimensionValueFilter> dedupRowDimValueFilterSupplier;

  @JsonCreator
  PartialDimensionDistributionTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory
  )
  {
    this(
        id,
        groupId,
        taskResource,
        supervisorTaskId,
        numAttempts,
        ingestionSchema,
        context,
        indexingServiceClient,
        taskClientFactory,
        () -> new DedupRowDimensionValueFilter(
            ingestionSchema.getDataSchema().getGranularitySpec().getQueryGranularity()
        )
    );
  }

  @VisibleForTesting  // Only for testing
  PartialDimensionDistributionTask(
      @Nullable String id,
      final String groupId,
      final TaskResource taskResource,
      final String supervisorTaskId,
      final int numAttempts,
      final ParallelIndexIngestionSpec ingestionSchema,
      final Map<String, Object> context,
      IndexingServiceClient indexingServiceClient,
      IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
      Supplier<DedupRowDimensionValueFilter> dedupRowDimValueFilterSupplier
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        ingestionSchema.getDataSchema(),
        ingestionSchema.getTuningConfig(),
        context
    );

    Preconditions.checkArgument(
        ingestionSchema.getTuningConfig().getPartitionsSpec() instanceof SingleDimensionPartitionsSpec,
        "%s partitionsSpec required",
        SingleDimensionPartitionsSpec.NAME
    );

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.indexingServiceClient = indexingServiceClient;
    this.taskClientFactory = taskClientFactory;
    this.dedupRowDimValueFilterSupplier = dedupRowDimValueFilterSupplier;
  }

  @JsonProperty
  private int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty("spec")
  private ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  private String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return tryTimeChunkLock(
        taskActionClient,
        getIngestionSchema().getDataSchema().getGranularitySpec().inputIntervals()
    );
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    DataSchema dataSchema = ingestionSchema.getDataSchema();
    GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();

    SingleDimensionPartitionsSpec partitionsSpec = (SingleDimensionPartitionsSpec) tuningConfig.getPartitionsSpec();
    Preconditions.checkNotNull(partitionsSpec, "partitionsSpec required in tuningConfig");
    String partitionDimension = partitionsSpec.getPartitionDimension();
    Preconditions.checkNotNull(partitionDimension, "partitionDimension required in partitionsSpec");
    boolean isAssumeGrouped = partitionsSpec.isAssumeGrouped();

    InputSource inputSource = ingestionSchema.getIOConfig().getNonNullInputSource(
        ingestionSchema.getDataSchema().getParser()
    );
    List<String> metricsNames = Arrays.stream(dataSchema.getAggregators())
                                      .map(AggregatorFactory::getName)
                                      .collect(Collectors.toList());
    InputFormat inputFormat = inputSource.needsFormat()
                              ? ParallelIndexSupervisorTask.getInputFormat(ingestionSchema)
                              : null;
    InputSourceReader inputSourceReader = dataSchema.getTransformSpec().decorate(
        inputSource.reader(
            new InputRowSchema(
                dataSchema.getTimestampSpec(),
                dataSchema.getDimensionsSpec(),
                metricsNames
            ),
            inputFormat,
            toolbox.getIndexingTmpDir()
        )
    );

    try (
        CloseableIterator<InputRow> inputRowIterator = inputSourceReader.read();
        HandlingInputRowIterator iterator = new RangePartitionIndexTaskInputRowIteratorBuilder(partitionDimension, SKIP_NULL)
            .delegate(inputRowIterator)
            .granularitySpec(granularitySpec)
            .nullRowRunnable(IndexTaskInputRowIteratorBuilder.NOOP_RUNNABLE)
            .absentBucketIntervalConsumer(IndexTaskInputRowIteratorBuilder.NOOP_CONSUMER)
            .build()
    ) {
      Map<Interval, StringDistribution> distribution = determineDistribution(
          iterator,
          granularitySpec,
          partitionDimension,
          isAssumeGrouped,
          tuningConfig.isLogParseExceptions(),
          tuningConfig.getMaxParseExceptions()
      );
      sendReport(new DimensionDistributionReport(getId(), distribution));
    }

    return TaskStatus.success(getId());
  }

  private Map<Interval, StringDistribution> determineDistribution(
      HandlingInputRowIterator inputRowIterator,
      GranularitySpec granularitySpec,
      String partitionDimension,
      boolean isAssumeGrouped,
      boolean isLogParseExceptions,
      int maxParseExceptions
  )
  {
    Map<Interval, StringDistribution> intervalToDistribution = new HashMap<>();
    DimensionValueFilter dimValueFilter =
        !isAssumeGrouped && granularitySpec.isRollup()
        ? dedupRowDimValueFilterSupplier.get()
        : new PassthroughRowDimensionValueFilter();

    int numParseExceptions = 0;

    while (inputRowIterator.hasNext()) {
      try {
        InputRow inputRow = inputRowIterator.next();
        if (inputRow == null) {
          continue;
        }

        DateTime timestamp = inputRow.getTimestamp();

        //noinspection OptionalGetWithoutIsPresent (InputRowIterator returns rows with present intervals)
        Interval interval = granularitySpec.bucketInterval(timestamp).get();
        StringDistribution stringDistribution =
            intervalToDistribution.computeIfAbsent(interval, k -> new StringSketch());

        String dimensionValue = dimValueFilter.accept(
            interval,
            timestamp,
            Iterables.getOnlyElement(inputRow.getDimension(partitionDimension))
        );

        if (dimensionValue != null) {
          stringDistribution.put(dimensionValue);
        }
      }
      catch (ParseException e) {
        if (isLogParseExceptions) {
          LOG.error(e, "Encountered parse exception");
        }

        numParseExceptions++;
        if (numParseExceptions > maxParseExceptions) {
          throw new RuntimeException("Max parse exceptions exceeded, terminating task...");
        }
      }
    }

    // DedupRowDimensionValueFilter may not accept the min/max dimensionValue. If needed, add the min/max
    // values to the distributions so they have an accurate min/max.
    dimValueFilter.getIntervalToMinDimensionValue()
                  .forEach((interval, min) -> intervalToDistribution.get(interval).putIfNewMin(min));
    dimValueFilter.getIntervalToMaxDimensionValue()
                  .forEach((interval, max) -> intervalToDistribution.get(interval).putIfNewMax(max));

    return intervalToDistribution;
  }

  private void sendReport(DimensionDistributionReport report)
  {
    final ParallelIndexSupervisorTaskClient taskClient = taskClientFactory.build(
        new ClientBasedTaskInfoProvider(indexingServiceClient),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );
    taskClient.report(supervisorTaskId, report);
  }

  private interface DimensionValueFilter
  {
    /**
     * @return Dimension value if it should be accepted, else null
     */
    @Nullable
    String accept(Interval interval, DateTime timestamp, String dimensionValue);

    /**
     * @return Minimum dimension value for each interval processed so far.
     */
    Map<Interval, String> getIntervalToMinDimensionValue();

    /**
     * @return Maximum dimension value for each interval processed so far.
     */
    Map<Interval, String> getIntervalToMaxDimensionValue();
  }

  /**
   * Filters out reoccurrences of rows that have timestamps with the same query granularity and dimension value.
   * Approximate matching is used, so there is a small probability that rows that are not reoccurences are discarded.
   */
  @VisibleForTesting
  static class DedupRowDimensionValueFilter implements DimensionValueFilter
  {
    // A bloom filter is used to approximately group rows by query granularity. These values assume
    // time chunks have fewer than BLOOM_FILTER_EXPECTED_INSERTIONS rows. With the below values, the
    // Bloom filter will use about 170MB of memory.
    //
    // For more details on the Bloom filter memory consumption:
    // https://github.com/google/guava/issues/2520#issuecomment-231233736
    private static final int BLOOM_FILTER_EXPECTED_INSERTIONS = 100_000_000;
    private static final double BLOOM_FILTER_EXPECTED_FALSE_POSITIVE_PROBABILTY = 0.001;

    private final PassthroughRowDimensionValueFilter delegate;
    private final TimeDimTupleFactory timeDimTupleFactory;
    private final BloomFilter<TimeDimTuple> timeDimTupleBloomFilter;

    DedupRowDimensionValueFilter(Granularity queryGranularity)
    {
      this(queryGranularity, BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_EXPECTED_FALSE_POSITIVE_PROBABILTY);
    }

    @VisibleForTesting  // to allow controlling false positive rate of bloom filter
    DedupRowDimensionValueFilter(
        Granularity queryGranularity,
        int bloomFilterExpectedInsertions,
        double bloomFilterFalsePositiveProbability
    )
    {
      delegate = new PassthroughRowDimensionValueFilter();
      timeDimTupleFactory = new TimeDimTupleFactory(queryGranularity);
      timeDimTupleBloomFilter = BloomFilter.create(
          TimeDimTupleFunnel.INSTANCE,
          bloomFilterExpectedInsertions,
          bloomFilterFalsePositiveProbability
      );
    }

    @Nullable
    @Override
    public String accept(Interval interval, DateTime timestamp, String dimensionValue)
    {
      delegate.accept(interval, timestamp, dimensionValue);

      TimeDimTuple timeDimTuple = timeDimTupleFactory.createWithBucketedTimestamp(timestamp, dimensionValue);
      if (timeDimTupleBloomFilter.mightContain(timeDimTuple)) {
        return null;
      } else {
        timeDimTupleBloomFilter.put(timeDimTuple);
        return dimensionValue;
      }
    }

    @Override
    public Map<Interval, String> getIntervalToMinDimensionValue()
    {
      return delegate.getIntervalToMinDimensionValue();
    }

    @Override
    public Map<Interval, String> getIntervalToMaxDimensionValue()
    {
      return delegate.getIntervalToMaxDimensionValue();
    }
  }

  /**
   * Accepts all input rows, even if they are reoccurrences of timestamps with the same query granularity and dimension
   * value.
   */
  private static class PassthroughRowDimensionValueFilter implements DimensionValueFilter
  {
    private final Map<Interval, String> intervalToMinDimensionValue;
    private final Map<Interval, String> intervalToMaxDimensionValue;

    PassthroughRowDimensionValueFilter()
    {
      this.intervalToMinDimensionValue = new HashMap<>();
      this.intervalToMaxDimensionValue = new HashMap<>();
    }

    @Override
    @Nullable
    public String accept(Interval interval, DateTime timestamp, String dimensionValue)
    {
      updateMinDimensionValue(interval, dimensionValue);
      updateMaxDimensionValue(interval, dimensionValue);
      return dimensionValue;
    }

    private void updateMinDimensionValue(Interval interval, String dimensionValue)
    {
      intervalToMinDimensionValue.compute(
          interval,
          (intervalKey, currentMinValue) -> {
            if (currentMinValue == null || dimensionValue.compareTo(currentMinValue) < 0) {
              return dimensionValue;
            } else {
              return currentMinValue;
            }
          }
      );
    }

    private void updateMaxDimensionValue(Interval interval, String dimensionValue)
    {
      intervalToMaxDimensionValue.compute(
          interval,
          (intervalKey, currentMaxValue) -> {
            if (currentMaxValue == null || dimensionValue.compareTo(currentMaxValue) > 0) {
              return dimensionValue;
            } else {
              return currentMaxValue;
            }
          }
      );
    }

    @Override
    public Map<Interval, String> getIntervalToMinDimensionValue()
    {
      return intervalToMinDimensionValue;
    }

    @Override
    public Map<Interval, String> getIntervalToMaxDimensionValue()
    {
      return intervalToMaxDimensionValue;
    }
  }
}
