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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.NoneGranularity;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CompactionTask extends AbstractTask
{
  private static final Logger log = new Logger(CompactionTask.class);
  private static final String TYPE = "compact";

  private final Interval interval;
  private final IndexTuningConfig tuningConfig;
  private final Injector injector;
  private final IndexIO indexIO;
  private final ObjectMapper jsonMapper;

  @JsonIgnore
  private IndexTask indexTaskSpec;

  @JsonCreator
  public CompactionTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") final Interval interval,
      @JsonProperty("tuningConfig") final IndexTuningConfig tuningConfig,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject Injector injector,
      @JacksonInject IndexIO indexIO,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(getOrMakeId(id, TYPE, dataSource), null, taskResource, dataSource, context);
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.tuningConfig = tuningConfig;
    this.injector = injector;
    this.indexIO = indexIO;
    this.jsonMapper = jsonMapper;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
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

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final SortedSet<Interval> intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    intervals.add(interval);
    return IndexTask.isReady(taskActionClient, intervals);
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    if (indexTaskSpec == null) {
      final IndexIngestionSpec ingestionSpec = createIngestionSchema(
          toolbox,
          getDataSource(),
          interval,
          tuningConfig,
          indexIO,
          injector,
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

    final String json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(indexTaskSpec);
    log.info("Generated compaction task details: " + json);

    return indexTaskSpec.run(toolbox);
  }

  private static IndexIngestionSpec createIngestionSchema(
      TaskToolbox toolbox,
      String dataSource,
      Interval interval,
      IndexTuningConfig tuningConfig,
      IndexIO indexIO,
      Injector injector,
      ObjectMapper jsonMapper
  ) throws IOException, SegmentLoadingException
  {
    Pair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> pair = prepareSegments(
        toolbox,
        dataSource,
        interval
    );
    final Map<DataSegment, File> segmentFileMap = pair.lhs;
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = pair.rhs;
    return new IndexIngestionSpec(
        createDataSchema(dataSource, interval, indexIO, jsonMapper, timelineSegments, segmentFileMap),
        new IndexIOConfig(
            new IngestSegmentFirehoseFactory(
                dataSource,
                interval,
                null, // no filter
                null, // null means all unique dimensions
                null, // null means all unique metrics
                injector,
                indexIO
            ),
            false
        ),
        tuningConfig
    );
  }

  private static Pair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> prepareSegments(
      TaskToolbox toolbox,
      String dataSource,
      Interval interval
  ) throws IOException, SegmentLoadingException
  {
    final List<DataSegment> usedSegments = toolbox
        .getTaskActionClient()
        .submit(new SegmentListUsedAction(dataSource, interval, null));
    final Map<DataSegment, File> segmentFileMap = toolbox.fetchSegments(usedSegments);
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = VersionedIntervalTimeline
        .forSegments(usedSegments)
        .lookup(interval);
    return Pair.of(segmentFileMap, timelineSegments);
  }

  private static DataSchema createDataSchema(
      String dataSource,
      Interval interval,
      IndexIO indexIO,
      ObjectMapper jsonMapper,
      List<TimelineObjectHolder<String, DataSegment>> timelineSegments,
      Map<DataSegment, File> segmentFileMap
  )
      throws IOException, SegmentLoadingException
  {
    // find metadata for interval
    final List<QueryableIndex> segments = loadSegments(timelineSegments, segmentFileMap, indexIO);

    // set rollup only if rollup is set for all segments
    final boolean rollup = segments.stream().allMatch(index -> index.getMetadata().isRollup());

    // find merged aggregators
    final List<AggregatorFactory[]> aggregatorFactories = segments.stream()
                                                                  .map(index -> index.getMetadata().getAggregators())
                                                                  .collect(Collectors.toList());
    final AggregatorFactory[] mergedAggregators = AggregatorFactory.mergeAggregators(aggregatorFactories);

    if (mergedAggregators == null) {
      throw new ISE("Failed to merge aggregators[%s]", aggregatorFactories);
    }

    // conver to combining aggregators
    final List<AggregatorFactory> combiningAggregatorList = Arrays.stream(mergedAggregators)
                                                                  .map(AggregatorFactory::getCombiningFactory)
                                                                  .collect(Collectors.toList());
    final AggregatorFactory[] combiningAggregators = combiningAggregatorList.toArray(
        new AggregatorFactory[combiningAggregatorList.size()]
    );

    // find granularity spec
    final GranularitySpec granularitySpec = new ArbitraryGranularitySpec(
        new NoneGranularity(),
        rollup,
        ImmutableList.of(interval)
    );

    return new DataSchema(
        dataSource,
        ImmutableMap.of("type", "noop"),
        combiningAggregators,
        granularitySpec,
        jsonMapper
    );
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
}
