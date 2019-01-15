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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.NoopInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class CompactionTask extends AbstractTask
{
  private static final Logger log = new Logger(CompactionTask.class);
  private static final String TYPE = "compact";

  private final Interval interval;
  private final List<DataSegment> segments;
  @Nullable
  private final DimensionsSpec dimensionsSpec;
  @Nullable
  private final AggregatorFactory[] metricsSpec;
  @Deprecated
  @Nullable
  private final Boolean keepSegmentGranularity;
  private final Granularity segmentGranularity;
  @Nullable
  private final Long targetCompactionSizeBytes;
  @Nullable
  private final IndexTuningConfig tuningConfig;
  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final SegmentProvider segmentProvider;
  @JsonIgnore
  private final PartitionConfigurationManager partitionConfigurationManager;

  @JsonIgnore
  private final AuthorizerMapper authorizerMapper;

  @JsonIgnore
  private final ChatHandlerProvider chatHandlerProvider;

  @JsonIgnore
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  @JsonIgnore
  private List<IndexTask> indexTaskSpecs;

  @JsonCreator
  public CompactionTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("dataSource") final String dataSource,
      @Nullable @JsonProperty("interval") final Interval interval,
      @Nullable @JsonProperty("segments") final List<DataSegment> segments,
      @Nullable @JsonProperty("dimensions") final DimensionsSpec dimensions,
      @Nullable @JsonProperty("dimensionsSpec") final DimensionsSpec dimensionsSpec,
      @Nullable @JsonProperty("metricsSpec") final AggregatorFactory[] metricsSpec,
      @Nullable @JsonProperty("keepSegmentGranularity") @Deprecated final Boolean keepSegmentGranularity,
      @Nullable @JsonProperty("segmentGranularity") final Granularity segmentGranularity,
      @Nullable @JsonProperty("targetCompactionSizeBytes") final Long targetCompactionSizeBytes,
      @Nullable @JsonProperty("tuningConfig") final IndexTuningConfig tuningConfig,
      @Nullable @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject ObjectMapper jsonMapper,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(getOrMakeId(id, TYPE, dataSource), null, taskResource, dataSource, context);
    Preconditions.checkArgument(interval != null || segments != null, "interval or segments should be specified");
    Preconditions.checkArgument(interval == null || segments == null, "one of interval and segments should be null");

    if (interval != null && interval.toDurationMillis() == 0) {
      throw new IAE("Interval[%s] is empty, must specify a nonempty interval", interval);
    }

    if ((keepSegmentGranularity != null && keepSegmentGranularity) && segmentGranularity != null) {
      throw new IAE("keepSegmentGranularity and segmentGranularity can't be used together");
    }

    if (keepSegmentGranularity != null) {
      log.warn("keepSegmentGranularity is deprecated. Set a proper segmentGranularity instead");
    }

    this.interval = interval;
    this.segments = segments;
    this.dimensionsSpec = dimensionsSpec == null ? dimensions : dimensionsSpec;
    this.metricsSpec = metricsSpec;
    this.keepSegmentGranularity = keepSegmentGranularity;
    this.segmentGranularity = segmentGranularity;
    this.targetCompactionSizeBytes = targetCompactionSizeBytes;
    this.tuningConfig = tuningConfig;
    this.jsonMapper = jsonMapper;
    this.segmentProvider = segments == null ? new SegmentProvider(dataSource, interval) : new SegmentProvider(segments);
    this.partitionConfigurationManager = new PartitionConfigurationManager(targetCompactionSizeBytes, tuningConfig);
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  @Deprecated
  @Nullable
  public Boolean isKeepSegmentGranularity()
  {
    return keepSegmentGranularity;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Nullable
  @JsonProperty
  public Long getTargetCompactionSizeBytes()
  {
    return targetCompactionSizeBytes;
  }

  @Nullable
  @JsonProperty
  public IndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_MERGE_TASK_PRIORITY);
  }

  @VisibleForTesting
  SegmentProvider getSegmentProvider()
  {
    return segmentProvider;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final SortedSet<Interval> intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    intervals.add(segmentProvider.interval);
    return IndexTask.isReady(taskActionClient, intervals);
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    if (indexTaskSpecs == null) {
      indexTaskSpecs = createIngestionSchema(
          toolbox,
          segmentProvider,
          partitionConfigurationManager,
          dimensionsSpec,
          metricsSpec,
          keepSegmentGranularity,
          segmentGranularity,
          jsonMapper
      ).stream()
      .map(spec -> new IndexTask(
          getId(),
          getGroupId(),
          getTaskResource(),
          getDataSource(),
          spec,
          getContext(),
          authorizerMapper,
          chatHandlerProvider,
          rowIngestionMetersFactory
      ))
      .collect(Collectors.toList());
    }

    if (indexTaskSpecs.isEmpty()) {
      log.warn("Interval[%s] has no segments, nothing to do.", interval);
      return TaskStatus.failure(getId());
    } else {
      final int totalNumSpecs = indexTaskSpecs.size();
      log.info("Generated [%d] compaction task specs", totalNumSpecs);

      int failCnt = 0;
      for (IndexTask eachSpec : indexTaskSpecs) {
        final String json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(eachSpec);
        log.info("Running indexSpec: " + json);

        try {
          final TaskStatus eachResult = eachSpec.run(toolbox);
          if (!eachResult.isSuccess()) {
            failCnt++;
            log.warn("Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
          }
        }
        catch (Exception e) {
          failCnt++;
          log.warn(e, "Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
        }
      }

      log.info("Run [%d] specs, [%d] succeeded, [%d] failed", totalNumSpecs, totalNumSpecs - failCnt, failCnt);
      return failCnt == 0 ? TaskStatus.success(getId()) : TaskStatus.failure(getId());
    }
  }

  /**
   * Generate {@link IndexIngestionSpec} from input segments.
   *
   * @return an empty list if input segments don't exist. Otherwise, a generated ingestionSpec.
   */
  @VisibleForTesting
  static List<IndexIngestionSpec> createIngestionSchema(
      final TaskToolbox toolbox,
      final SegmentProvider segmentProvider,
      final PartitionConfigurationManager partitionConfigurationManager,
      @Nullable final DimensionsSpec dimensionsSpec,
      @Nullable final AggregatorFactory[] metricsSpec,
      @Nullable final Boolean keepSegmentGranularity,
      @Nullable final Granularity segmentGranularity,
      final ObjectMapper jsonMapper
  ) throws IOException, SegmentLoadingException
  {
    Pair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> pair = prepareSegments(
        toolbox,
        segmentProvider
    );
    final Map<DataSegment, File> segmentFileMap = pair.lhs;
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = pair.rhs;

    if (timelineSegments.size() == 0) {
      return Collections.emptyList();
    }

    // find metadata for interval
    final List<Pair<QueryableIndex, DataSegment>> queryableIndexAndSegments = loadSegments(
        timelineSegments,
        segmentFileMap,
        toolbox.getIndexIO()
    );

    final IndexTuningConfig compactionTuningConfig = partitionConfigurationManager.computeTuningConfig(
        queryableIndexAndSegments
    );

    if (segmentGranularity == null) {
      if (keepSegmentGranularity != null && !keepSegmentGranularity) {
        // all granularity
        final DataSchema dataSchema = createDataSchema(
            segmentProvider.dataSource,
            segmentProvider.interval,
            queryableIndexAndSegments,
            dimensionsSpec,
            metricsSpec,
            Granularities.ALL,
            jsonMapper
        );

        return Collections.singletonList(
            new IndexIngestionSpec(
                dataSchema,
                createIoConfig(toolbox, dataSchema, segmentProvider.interval),
                compactionTuningConfig
            )
        );
      } else {
        // original granularity
        final Map<Interval, List<Pair<QueryableIndex, DataSegment>>> intervalToSegments = new TreeMap<>(
            Comparators.intervalsByStartThenEnd()
        );
        //noinspection ConstantConditions
        queryableIndexAndSegments.forEach(
            p -> intervalToSegments.computeIfAbsent(p.rhs.getInterval(), k -> new ArrayList<>())
                                   .add(p)
        );

        final List<IndexIngestionSpec> specs = new ArrayList<>(intervalToSegments.size());
        for (Entry<Interval, List<Pair<QueryableIndex, DataSegment>>> entry : intervalToSegments.entrySet()) {
          final Interval interval = entry.getKey();
          final List<Pair<QueryableIndex, DataSegment>> segmentsToCompact = entry.getValue();
          final DataSchema dataSchema = createDataSchema(
              segmentProvider.dataSource,
              interval,
              segmentsToCompact,
              dimensionsSpec,
              metricsSpec,
              GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity(),
              jsonMapper
          );

          specs.add(
              new IndexIngestionSpec(
                  dataSchema,
                  createIoConfig(toolbox, dataSchema, interval),
                  compactionTuningConfig
              )
          );
        }

        return specs;
      }
    } else {
      if (keepSegmentGranularity != null && keepSegmentGranularity) {
        // error
        throw new ISE("segmentGranularity[%s] and keepSegmentGranularity can't be used together", segmentGranularity);
      } else {
        // given segment granularity
        final DataSchema dataSchema = createDataSchema(
            segmentProvider.dataSource,
            segmentProvider.interval,
            queryableIndexAndSegments,
            dimensionsSpec,
            metricsSpec,
            segmentGranularity,
            jsonMapper
        );

        return Collections.singletonList(
            new IndexIngestionSpec(
                dataSchema,
                createIoConfig(toolbox, dataSchema, segmentProvider.interval),
                compactionTuningConfig
            )
        );
      }
    }
  }

  private static IndexIOConfig createIoConfig(TaskToolbox toolbox, DataSchema dataSchema, Interval interval)
  {
    return new IndexIOConfig(
        new IngestSegmentFirehoseFactory(
            dataSchema.getDataSource(),
            interval,
            null, // no filter
            // set dimensions and metrics names to make sure that the generated dataSchema is used for the firehose
            dataSchema.getParser().getParseSpec().getDimensionsSpec().getDimensionNames(),
            Arrays.stream(dataSchema.getAggregators()).map(AggregatorFactory::getName).collect(Collectors.toList()),
            toolbox.getIndexIO()
        ),
        false
    );
  }

  private static Pair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> prepareSegments(
      TaskToolbox toolbox,
      SegmentProvider segmentProvider
  ) throws IOException, SegmentLoadingException
  {
    final List<DataSegment> usedSegments = segmentProvider.checkAndGetSegments(toolbox);
    final Map<DataSegment, File> segmentFileMap = toolbox.fetchSegments(usedSegments);
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = VersionedIntervalTimeline
        .forSegments(usedSegments)
        .lookup(segmentProvider.interval);
    return Pair.of(segmentFileMap, timelineSegments);
  }

  private static DataSchema createDataSchema(
      String dataSource,
      Interval totalInterval,
      List<Pair<QueryableIndex, DataSegment>> queryableIndexAndSegments,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      Granularity segmentGranularity,
      ObjectMapper jsonMapper
  )
  {
    // check index metadata
    for (Pair<QueryableIndex, DataSegment> pair : queryableIndexAndSegments) {
      final QueryableIndex index = pair.lhs;
      if (index.getMetadata() == null) {
        throw new RE("Index metadata doesn't exist for segment[%s]", pair.rhs.getId());
      }
    }

    // find granularity spec
    // set rollup only if rollup is set for all segments
    final boolean rollup = queryableIndexAndSegments.stream().allMatch(pair -> {
      // We have already checked getMetadata() doesn't return null
      final Boolean isRollup = pair.lhs.getMetadata().isRollup();
      return isRollup != null && isRollup;
    });

    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Preconditions.checkNotNull(segmentGranularity),
        Granularities.NONE,
        rollup,
        Collections.singletonList(totalInterval)
    );

    // find unique dimensions
    final DimensionsSpec finalDimensionsSpec = dimensionsSpec == null
                                               ? createDimensionsSpec(queryableIndexAndSegments)
                                               : dimensionsSpec;
    final AggregatorFactory[] finalMetricsSpec = metricsSpec == null
                                                 ? createMetricsSpec(queryableIndexAndSegments)
                                                 : convertToCombiningFactories(metricsSpec);
    final InputRowParser parser = new NoopInputRowParser(new TimeAndDimsParseSpec(null, finalDimensionsSpec));

    return new DataSchema(
        dataSource,
        jsonMapper.convertValue(parser, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        finalMetricsSpec,
        granularitySpec,
        null,
        jsonMapper
    );
  }

  private static AggregatorFactory[] createMetricsSpec(
      List<Pair<QueryableIndex, DataSegment>> queryableIndexAndSegments
  )
  {
    final List<AggregatorFactory[]> aggregatorFactories = queryableIndexAndSegments
        .stream()
        .map(pair -> pair.lhs.getMetadata().getAggregators()) // We have already done null check on index.getMetadata()
        .collect(Collectors.toList());
    final AggregatorFactory[] mergedAggregators = AggregatorFactory.mergeAggregators(aggregatorFactories);

    if (mergedAggregators == null) {
      throw new ISE("Failed to merge aggregators[%s]", aggregatorFactories);
    }
    return mergedAggregators;
  }

  private static AggregatorFactory[] convertToCombiningFactories(AggregatorFactory[] metricsSpec)
  {
    return Arrays.stream(metricsSpec)
                 .map(AggregatorFactory::getCombiningFactory)
                 .toArray(AggregatorFactory[]::new);
  }

  private static DimensionsSpec createDimensionsSpec(List<Pair<QueryableIndex, DataSegment>> queryableIndices)
  {
    final BiMap<String, Integer> uniqueDims = HashBiMap.create();
    final Map<String, DimensionSchema> dimensionSchemaMap = new HashMap<>();

    // Here, we try to retain the order of dimensions as they were specified since the order of dimensions may be
    // optimized for performance.
    // Dimensions are extracted from the recent segments to olders because recent segments are likely to be queried more
    // frequently, and thus the performance should be optimized for recent ones rather than old ones.

    // timelineSegments are sorted in order of interval, but we do a sanity check here.
    final Comparator<Interval> intervalComparator = Comparators.intervalsByStartThenEnd();
    for (int i = 0; i < queryableIndices.size() - 1; i++) {
      final Interval shouldBeSmaller = queryableIndices.get(i).lhs.getDataInterval();
      final Interval shouldBeLarger = queryableIndices.get(i + 1).lhs.getDataInterval();
      Preconditions.checkState(
          intervalComparator.compare(shouldBeSmaller, shouldBeLarger) <= 0,
          "QueryableIndexes are not sorted! Interval[%s] of segment[%s] is laster than interval[%s] of segment[%s]",
          shouldBeSmaller,
          queryableIndices.get(i).rhs.getId(),
          shouldBeLarger,
          queryableIndices.get(i + 1).rhs.getId()
      );
    }

    int index = 0;
    for (Pair<QueryableIndex, DataSegment> pair : Lists.reverse(queryableIndices)) {
      final QueryableIndex queryableIndex = pair.lhs;
      final Map<String, DimensionHandler> dimensionHandlerMap = queryableIndex.getDimensionHandlers();

      for (String dimension : queryableIndex.getAvailableDimensions()) {
        final ColumnHolder columnHolder = Preconditions.checkNotNull(
            queryableIndex.getColumnHolder(dimension),
            "Cannot find column for dimension[%s]",
            dimension
        );

        if (!uniqueDims.containsKey(dimension)) {
          final DimensionHandler dimensionHandler = Preconditions.checkNotNull(
              dimensionHandlerMap.get(dimension),
              "Cannot find dimensionHandler for dimension[%s]",
              dimension
          );

          uniqueDims.put(dimension, index++);
          dimensionSchemaMap.put(
              dimension,
              createDimensionSchema(
                  columnHolder.getCapabilities().getType(),
                  dimension,
                  dimensionHandler.getMultivalueHandling(),
                  columnHolder.getCapabilities().hasBitmapIndexes()
              )
          );
        }
      }
    }

    final BiMap<Integer, String> orderedDims = uniqueDims.inverse();
    final List<DimensionSchema> dimensionSchemas = IntStream.range(0, orderedDims.size())
                                                            .mapToObj(i -> {
                                                              final String dimName = orderedDims.get(i);
                                                              return Preconditions.checkNotNull(
                                                                  dimensionSchemaMap.get(dimName),
                                                                  "Cannot find dimension[%s] from dimensionSchemaMap",
                                                                  dimName
                                                              );
                                                            })
                                                            .collect(Collectors.toList());

    return new DimensionsSpec(dimensionSchemas, null, null);
  }

  private static List<Pair<QueryableIndex, DataSegment>> loadSegments(
      List<TimelineObjectHolder<String, DataSegment>> timelineObjectHolders,
      Map<DataSegment, File> segmentFileMap,
      IndexIO indexIO
  ) throws IOException
  {
    final List<Pair<QueryableIndex, DataSegment>> segments = new ArrayList<>();

    for (TimelineObjectHolder<String, DataSegment> timelineObjectHolder : timelineObjectHolders) {
      final PartitionHolder<DataSegment> partitionHolder = timelineObjectHolder.getObject();
      for (PartitionChunk<DataSegment> chunk : partitionHolder) {
        final DataSegment segment = chunk.getObject();
        final QueryableIndex queryableIndex = indexIO.loadIndex(
            Preconditions.checkNotNull(segmentFileMap.get(segment), "File for segment %s", segment.getId())
        );
        segments.add(Pair.of(queryableIndex, segment));
      }
    }

    return segments;
  }

  private static DimensionSchema createDimensionSchema(
      ValueType type,
      String name,
      MultiValueHandling multiValueHandling,
      boolean hasBitmapIndexes
  )
  {
    switch (type) {
      case FLOAT:
        Preconditions.checkArgument(
            multiValueHandling == null,
            "multi-value dimension [%s] is not supported for float type yet",
            name
        );
        return new FloatDimensionSchema(name);
      case LONG:
        Preconditions.checkArgument(
            multiValueHandling == null,
            "multi-value dimension [%s] is not supported for long type yet",
            name
        );
        return new LongDimensionSchema(name);
      case DOUBLE:
        Preconditions.checkArgument(
            multiValueHandling == null,
            "multi-value dimension [%s] is not supported for double type yet",
            name
        );
        return new DoubleDimensionSchema(name);
      case STRING:
        return new StringDimensionSchema(name, multiValueHandling, hasBitmapIndexes);
      default:
        throw new ISE("Unsupported value type[%s] for dimension[%s]", type, name);
    }
  }

  @VisibleForTesting
  static class SegmentProvider
  {
    private final String dataSource;
    private final Interval interval;
    private final List<DataSegment> segments;

    SegmentProvider(String dataSource, Interval interval)
    {
      this.dataSource = Preconditions.checkNotNull(dataSource);
      this.interval = Preconditions.checkNotNull(interval);
      this.segments = null;
    }

    SegmentProvider(List<DataSegment> segments)
    {
      Preconditions.checkArgument(segments != null && !segments.isEmpty());
      final String dataSource = segments.get(0).getDataSource();
      Preconditions.checkArgument(
          segments.stream().allMatch(segment -> segment.getDataSource().equals(dataSource)),
          "segments should have the same dataSource"
      );
      this.segments = segments;
      this.dataSource = dataSource;
      this.interval = JodaUtils.umbrellaInterval(
          segments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
      );
    }

    List<DataSegment> getSegments()
    {
      return segments;
    }

    List<DataSegment> checkAndGetSegments(TaskToolbox toolbox) throws IOException
    {
      final List<DataSegment> usedSegments = toolbox.getTaskActionClient().submit(
          new SegmentListUsedAction(dataSource, interval, null)
      );
      final TimelineLookup<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(usedSegments);
      final List<DataSegment> latestSegments = timeline
          .lookup(interval)
          .stream()
          .map(TimelineObjectHolder::getObject)
          .flatMap(partitionHolder -> StreamSupport.stream(partitionHolder.spliterator(), false))
          .map(PartitionChunk::getObject)
          .collect(Collectors.toList());

      if (segments != null) {
        Collections.sort(latestSegments);
        Collections.sort(segments);

        if (!latestSegments.equals(segments)) {
          final List<DataSegment> unknownSegments = segments.stream()
                                                            .filter(segment -> !latestSegments.contains(segment))
                                                            .collect(Collectors.toList());
          final List<DataSegment> missingSegments = latestSegments.stream()
                                                                  .filter(segment -> !segments.contains(segment))
                                                                  .collect(Collectors.toList());
          throw new ISE(
              "Specified segments in the spec are different from the current used segments. "
              + "There are unknown segments[%s] and missing segments[%s] in the spec.",
              unknownSegments,
              missingSegments
          );
        }
      }
      return latestSegments;
    }
  }

  @VisibleForTesting
  static class PartitionConfigurationManager
  {
    @Nullable
    private final Long targetCompactionSizeBytes;
    @Nullable
    private final IndexTuningConfig tuningConfig;

    PartitionConfigurationManager(@Nullable Long targetCompactionSizeBytes, @Nullable IndexTuningConfig tuningConfig)
    {
      this.targetCompactionSizeBytes = getValidTargetCompactionSizeBytes(targetCompactionSizeBytes, tuningConfig);
      this.tuningConfig = tuningConfig;
    }

    @Nullable
    IndexTuningConfig computeTuningConfig(List<Pair<QueryableIndex, DataSegment>> queryableIndexAndSegments)
    {
      if (!hasPartitionConfig(tuningConfig)) {
        final long nonNullTargetCompactionSizeBytes = Preconditions.checkNotNull(
            targetCompactionSizeBytes,
            "targetCompactionSizeBytes"
        );
        // Find IndexTuningConfig.maxRowsPerSegment which is the number of rows per segment.
        // Assume that the segment size is proportional to the number of rows. We can improve this later.
        final long totalNumRows = queryableIndexAndSegments
            .stream()
            .mapToLong(queryableIndexAndDataSegment -> queryableIndexAndDataSegment.lhs.getNumRows())
            .sum();
        final long totalSizeBytes = queryableIndexAndSegments
            .stream()
            .mapToLong(queryableIndexAndDataSegment -> queryableIndexAndDataSegment.rhs.getSize())
            .sum();

        if (totalSizeBytes == 0L) {
          throw new ISE("Total input segment size is 0 byte");
        }

        final double avgRowsPerByte = totalNumRows / (double) totalSizeBytes;
        final int maxRowsPerSegment = Math.toIntExact(Math.round(avgRowsPerByte * nonNullTargetCompactionSizeBytes));
        Preconditions.checkState(maxRowsPerSegment > 0, "Negative maxRowsPerSegment[%s]", maxRowsPerSegment);

        log.info(
            "Estimated maxRowsPerSegment[%d] = avgRowsPerByte[%f] * targetCompactionSizeBytes[%d]",
            maxRowsPerSegment,
            avgRowsPerByte,
            nonNullTargetCompactionSizeBytes
        );
        return (tuningConfig == null ? IndexTuningConfig.createDefault() : tuningConfig)
            .withMaxRowsPerSegment(maxRowsPerSegment);
      } else {
        return tuningConfig;
      }
    }

    /**
     * Check the validity of {@link #targetCompactionSizeBytes} and return a valid value. Note that
     * targetCompactionSizeBytes cannot be used with {@link IndexTuningConfig#maxRowsPerSegment},
     * {@link IndexTuningConfig#maxTotalRows}, or {@link IndexTuningConfig#numShards} together.
     * {@link #hasPartitionConfig} checks one of those configs is set.
     *
     * This throws an {@link IllegalArgumentException} if targetCompactionSizeBytes is set and hasPartitionConfig
     * returns true. If targetCompactionSizeBytes is not set, this returns null or
     * {@link DataSourceCompactionConfig#DEFAULT_TARGET_COMPACTION_SIZE_BYTES} according to the result of
     * hasPartitionConfig.
     */
    @Nullable
    private static Long getValidTargetCompactionSizeBytes(
        @Nullable Long targetCompactionSizeBytes,
        @Nullable IndexTuningConfig tuningConfig
    )
    {
      if (targetCompactionSizeBytes != null && tuningConfig != null) {
        Preconditions.checkArgument(
            !hasPartitionConfig(tuningConfig),
            "targetCompactionSizeBytes[%s] cannot be used with maxRowsPerSegment[%s], maxTotalRows[%s],"
            + " or numShards[%s] of tuningConfig",
            targetCompactionSizeBytes,
            tuningConfig.getMaxRowsPerSegment(),
            tuningConfig.getMaxTotalRows(),
            tuningConfig.getNumShards()
        );
        return targetCompactionSizeBytes;
      } else {
        return hasPartitionConfig(tuningConfig)
               ? null
               : DataSourceCompactionConfig.DEFAULT_TARGET_COMPACTION_SIZE_BYTES;
      }
    }

    private static boolean hasPartitionConfig(@Nullable IndexTuningConfig tuningConfig)
    {
      if (tuningConfig != null) {
        return tuningConfig.getMaxRowsPerSegment() != null
               || tuningConfig.getMaxTotalRows() != null
               || tuningConfig.getNumShards() != null;
      } else {
        return false;
      }
    }
  }

  public static class Builder
  {
    private final String dataSource;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;
    private final ChatHandlerProvider chatHandlerProvider;
    private final RowIngestionMetersFactory rowIngestionMetersFactory;

    @Nullable
    private Interval interval;
    @Nullable
    private List<DataSegment> segments;
    @Nullable
    private DimensionsSpec dimensionsSpec;
    @Nullable
    private AggregatorFactory[] metricsSpec;
    @Nullable
    private Boolean keepSegmentGranularity;
    @Nullable
    private Granularity segmentGranularity;
    @Nullable
    private Long targetCompactionSizeBytes;
    @Nullable
    private IndexTuningConfig tuningConfig;
    @Nullable
    private Map<String, Object> context;

    public Builder(
        String dataSource,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper,
        ChatHandlerProvider chatHandlerProvider,
        RowIngestionMetersFactory rowIngestionMetersFactory
    )
    {
      this.dataSource = dataSource;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
      this.chatHandlerProvider = chatHandlerProvider;
      this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    }

    public Builder interval(Interval interval)
    {
      this.interval = interval;
      return this;
    }

    public Builder segments(List<DataSegment> segments)
    {
      this.segments = segments;
      return this;
    }

    public Builder dimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder metricsSpec(AggregatorFactory[] metricsSpec)
    {
      this.metricsSpec = metricsSpec;
      return this;
    }

    public Builder keepSegmentGranularity(boolean keepSegmentGranularity)
    {
      this.keepSegmentGranularity = keepSegmentGranularity;
      return this;
    }

    public Builder segmentGranularity(Granularity segmentGranularity)
    {
      this.segmentGranularity = segmentGranularity;
      return this;
    }

    public Builder targetCompactionSizeBytes(long targetCompactionSizeBytes)
    {
      this.targetCompactionSizeBytes = targetCompactionSizeBytes;
      return this;
    }

    public Builder tuningConfig(IndexTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder context(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public CompactionTask build()
    {
      return new CompactionTask(
          null,
          null,
          dataSource,
          interval,
          segments,
          null,
          dimensionsSpec,
          metricsSpec,
          keepSegmentGranularity,
          segmentGranularity,
          targetCompactionSizeBytes,
          tuningConfig,
          context,
          jsonMapper,
          authorizerMapper,
          chatHandlerProvider,
          rowIngestionMetersFactory
      );
    }
  }
}
