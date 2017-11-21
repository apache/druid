/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.DoubleDimensionSchema;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.NoopInputRowParser;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.NoneGranularity;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.DimensionHandler;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CompactionTask extends AbstractTask
{
  private static final Logger log = new Logger(CompactionTask.class);
  private static final String TYPE = "compact";

  private final Interval interval;
  private final List<DataSegment> segments;
  private final DimensionsSpec dimensionsSpec;
  private final IndexTuningConfig tuningConfig;
  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final SegmentProvider segmentProvider;

  @JsonIgnore
  private IndexTask indexTaskSpec;

  @JsonCreator
  public CompactionTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("dataSource") final String dataSource,
      @Nullable @JsonProperty("interval") final Interval interval,
      @Nullable @JsonProperty("segments") final List<DataSegment> segments,
      @Nullable @JsonProperty("dimensions") final DimensionsSpec dimensionsSpec,
      @Nullable @JsonProperty("tuningConfig") final IndexTuningConfig tuningConfig,
      @Nullable @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(getOrMakeId(id, TYPE, dataSource), null, taskResource, dataSource, context);
    Preconditions.checkArgument(interval != null || segments != null, "interval or segments should be specified");
    Preconditions.checkArgument(interval == null || segments == null, "one of interval and segments should be null");

    this.interval = interval;
    this.segments = segments;
    this.dimensionsSpec = dimensionsSpec;
    this.tuningConfig = tuningConfig;
    this.jsonMapper = jsonMapper;
    this.segmentProvider = segments == null ? new SegmentProvider(dataSource, interval) : new SegmentProvider(segments);
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
    if (indexTaskSpec == null) {
      final IndexIngestionSpec ingestionSpec = createIngestionSchema(
          toolbox,
          segmentProvider,
          dimensionsSpec,
          tuningConfig,
          jsonMapper
      );

      indexTaskSpec = new IndexTask(
          getId(),
          getGroupId(),
          getTaskResource(),
          getDataSource(),
          ingestionSpec,
          getContext()
      );
    }

    if (indexTaskSpec.getIngestionSchema() == null) {
      log.info("Cannot find segments for interval");
    }

    final String json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(indexTaskSpec);
    log.info("Generated compaction task details: " + json);

    return indexTaskSpec.run(toolbox);
  }

  @VisibleForTesting
  static IndexIngestionSpec createIngestionSchema(
      TaskToolbox toolbox,
      SegmentProvider segmentProvider,
      DimensionsSpec dimensionsSpec,
      IndexTuningConfig tuningConfig,
      ObjectMapper jsonMapper
  ) throws IOException, SegmentLoadingException
  {
    Pair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> pair = prepareSegments(
        toolbox,
        segmentProvider
    );
    final Map<DataSegment, File> segmentFileMap = pair.lhs;
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = pair.rhs;

    if (timelineSegments.size() == 0) {
      return null;
    }

    final DataSchema dataSchema = createDataSchema(
        segmentProvider.dataSource,
        segmentProvider.interval,
        dimensionsSpec,
        toolbox.getIndexIO(),
        jsonMapper,
        timelineSegments,
        segmentFileMap
    );
    return new IndexIngestionSpec(
        dataSchema,
        new IndexIOConfig(
            new IngestSegmentFirehoseFactory(
                segmentProvider.dataSource,
                segmentProvider.interval,
                null, // no filter
                // set dimensions and metrics names to make sure that the generated dataSchema is used for the firehose
                dataSchema.getParser().getParseSpec().getDimensionsSpec().getDimensionNames(),
                Arrays.stream(dataSchema.getAggregators()).map(AggregatorFactory::getName).collect(Collectors.toList()),
                toolbox.getIndexIO()
            ),
            false
        ),
        tuningConfig
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
      Interval interval,
      DimensionsSpec dimensionsSpec,
      IndexIO indexIO,
      ObjectMapper jsonMapper,
      List<TimelineObjectHolder<String, DataSegment>> timelineSegments,
      Map<DataSegment, File> segmentFileMap
  )
      throws IOException, SegmentLoadingException
  {
    // find metadata for interval
    final List<QueryableIndex> queryableIndices = loadSegments(timelineSegments, segmentFileMap, indexIO);

    // find merged aggregators
    final List<AggregatorFactory[]> aggregatorFactories = queryableIndices
        .stream()
        .map(index -> index.getMetadata().getAggregators())
        .collect(Collectors.toList());
    final AggregatorFactory[] mergedAggregators = AggregatorFactory.mergeAggregators(aggregatorFactories);

    if (mergedAggregators == null) {
      throw new ISE("Failed to merge aggregators[%s]", aggregatorFactories);
    }

    // find granularity spec
    // set rollup only if rollup is set for all segments
    final boolean rollup = queryableIndices.stream().allMatch(index -> index.getMetadata().isRollup());
    final GranularitySpec granularitySpec = new ArbitraryGranularitySpec(
        new NoneGranularity(),
        rollup,
        ImmutableList.of(interval)
    );

    // find unique dimensions
    final DimensionsSpec finalDimensionsSpec = dimensionsSpec == null ?
                                               createDimensionsSpec(queryableIndices) :
                                               dimensionsSpec;
    final InputRowParser parser = new NoopInputRowParser(new TimeAndDimsParseSpec(null, finalDimensionsSpec));

    return new DataSchema(
        dataSource,
        jsonMapper.convertValue(parser, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        mergedAggregators,
        granularitySpec,
        null,
        jsonMapper
    );
  }

  private static DimensionsSpec createDimensionsSpec(List<QueryableIndex> queryableIndices)
  {
    final BiMap<String, Integer> uniqueDims = HashBiMap.create();
    final Map<String, DimensionSchema> dimensionSchemaMap = new HashMap<>();

    // Here, we try to retain the order of dimensions as they were specified since the order of dimensions may be
    // optimized for performance.
    // Dimensions are extracted from the recent segments to olders because recent segments are likely to be queried more
    // frequently, and thus the performance should be optimized for recent ones rather than old ones.

    // timelineSegments are sorted in order of interval
    int index = 0;
    for (QueryableIndex queryableIndex : Lists.reverse(queryableIndices)) {
      final Map<String, DimensionHandler> dimensionHandlerMap = queryableIndex.getDimensionHandlers();

      for (String dimension : queryableIndex.getAvailableDimensions()) {
        final Column column = Preconditions.checkNotNull(
            queryableIndex.getColumn(dimension),
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
                  column.getCapabilities().getType(),
                  dimension,
                  dimensionHandler.getMultivalueHandling()
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

  private static List<QueryableIndex> loadSegments(
      List<TimelineObjectHolder<String, DataSegment>> timelineSegments,
      Map<DataSegment, File> segmentFileMap,
      IndexIO indexIO
  ) throws IOException
  {
    final List<QueryableIndex> segments = new ArrayList<>();

    for (TimelineObjectHolder<String, DataSegment> timelineSegment : timelineSegments) {
      final PartitionHolder<DataSegment> partitionHolder = timelineSegment.getObject();
      for (PartitionChunk<DataSegment> chunk : partitionHolder) {
        final DataSegment segment = chunk.getObject();
        segments.add(
            indexIO.loadIndex(
                Preconditions.checkNotNull(segmentFileMap.get(segment), "File for segment %s", segment.getIdentifier())
            )
        );
      }
    }

    return segments;
  }

  private static DimensionSchema createDimensionSchema(
      ValueType type,
      String name,
      MultiValueHandling multiValueHandling
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
        return new StringDimensionSchema(name, multiValueHandling);
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
      final List<DataSegment> usedSegments = toolbox.getTaskActionClient()
                                                    .submit(new SegmentListUsedAction(dataSource, interval, null));
      if (segments != null) {
        Collections.sort(usedSegments);
        Collections.sort(segments);
        Preconditions.checkState(
            usedSegments.equals(segments),
            "Specified segments[%s] are different from the current used segments[%s]",
            segments,
            usedSegments
        );
      }
      return usedSegments;
    }
  }
}
