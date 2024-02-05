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

package org.apache.druid.indexing.input;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.utils.Streams;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * An {@link InputSource} that allows reading from Druid segments.
 *
 * Used internally by {@link org.apache.druid.indexing.common.task.CompactionTask}, and can also be used directly.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DruidInputSource extends AbstractInputSource implements SplittableInputSource<List<WindowedSegmentId>>, TaskInputSource
{

  public static final String TYPE_KEY = "druid";
  private static final Logger LOG = new Logger(DruidInputSource.class);

  /**
   * Timestamp formats that the standard __time column can be parsed with.
   */
  private static final List<String> STANDARD_TIME_COLUMN_FORMATS = ImmutableList.of("millis", "auto");

  /**
   * A Comparator that orders {@link WindowedSegmentId} mainly by segmentId (which is important), and then by intervals
   * (which is arbitrary, and only here for totality of ordering).
   */
  private static final Comparator<WindowedSegmentId> WINDOWED_SEGMENT_ID_COMPARATOR =
      Comparator.comparing(WindowedSegmentId::getSegmentId)
                .thenComparing(windowedSegmentId -> windowedSegmentId.getIntervals().size())
                .thenComparing(
                    (WindowedSegmentId a, WindowedSegmentId b) -> {
                      // Same segmentId, same intervals list size. Compare each interval.
                      int cmp = 0;

                      for (int i = 0; i < a.getIntervals().size(); i++) {
                        cmp = Comparators.intervalsByStartThenEnd()
                                         .compare(a.getIntervals().get(i), b.getIntervals().get(i));

                        if (cmp != 0) {
                          return cmp;
                        }
                      }

                      return cmp;
                    }
                );

  private final String dataSource;
  // Exactly one of interval and segmentIds should be non-null. Typically 'interval' is specified directly
  // by the user creating this firehose and 'segmentIds' is used for sub-tasks if it is split for parallel
  // batch ingestion.
  @Nullable
  private final Interval interval;
  @Nullable
  private final List<WindowedSegmentId> segmentIds;
  private final DimFilter dimFilter;
  private final IndexIO indexIO;
  private final CoordinatorClient coordinatorClient;
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;
  private final TaskConfig taskConfig;

  /**
   * Included for serde backwards-compatibility only. Not used.
   */
  private final List<String> dimensions;

  /**
   * Included for serde backwards-compatibility only. Not used.
   */
  private final List<String> metrics;

  @Nullable
  private final TaskToolbox toolbox;

  @JsonCreator
  public DruidInputSource(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") @Nullable Interval interval,
      // Specifying "segments" is intended only for when this FirehoseFactory has split itself,
      // not for direct end user use.
      @JsonProperty("segments") @Nullable List<WindowedSegmentId> segmentIds,
      @JsonProperty("filter") DimFilter dimFilter,
      @Nullable @JsonProperty("dimensions") List<String> dimensions,
      @Nullable @JsonProperty("metrics") List<String> metrics,
      @JacksonInject IndexIO indexIO,
      @JacksonInject CoordinatorClient coordinatorClient,
      @JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory,
      @JacksonInject TaskConfig taskConfig
  )
  {
    this(
        dataSource,
        interval,
        segmentIds,
        dimFilter,
        dimensions,
        metrics,
        indexIO,
        coordinatorClient,
        segmentCacheManagerFactory,
        taskConfig,
        null
    );
  }

  private DruidInputSource(
      final String dataSource,
      @Nullable Interval interval,
      @Nullable List<WindowedSegmentId> segmentIds,
      DimFilter dimFilter,
      List<String> dimensions,
      List<String> metrics,
      IndexIO indexIO,
      CoordinatorClient coordinatorClient,
      SegmentCacheManagerFactory segmentCacheManagerFactory,
      TaskConfig taskConfig,
      @Nullable TaskToolbox toolbox
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    if ((interval == null && segmentIds == null) || (interval != null && segmentIds != null)) {
      throw new IAE("Specify exactly one of 'interval' and 'segments'");
    }
    this.dataSource = dataSource;
    this.interval = interval;
    this.segmentIds = segmentIds;
    this.dimFilter = dimFilter;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
    this.coordinatorClient = Preconditions.checkNotNull(coordinatorClient, "null CoordinatorClient");
    this.segmentCacheManagerFactory = Preconditions.checkNotNull(segmentCacheManagerFactory, "null segmentCacheManagerFactory");
    this.taskConfig = Preconditions.checkNotNull(taskConfig, "null taskConfig");
    this.toolbox = toolbox;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public Set<String> getTypes()
  {
    return ImmutableSet.of(TYPE_KEY);
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @Nullable
  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  @JsonProperty("segments")
  public List<WindowedSegmentId> getSegmentIds()
  {
    return segmentIds;
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  /**
   * Included for serde backwards-compatibility only. Not used.
   */
  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  /**
   * Included for serde backwards-compatibility only. Not used.
   */
  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @Override
  public InputSource withTaskToolbox(TaskToolbox toolbox)
  {
    return new DruidInputSource(
        this.dataSource,
        this.interval,
        this.segmentIds,
        this.dimFilter,
        this.dimensions,
        this.metrics,
        this.indexIO,
        this.coordinatorClient,
        this.segmentCacheManagerFactory,
        this.taskConfig,
        toolbox
    );
  }

  @Override
  protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
  {
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(temporaryDirectory);

    final List<TimelineObjectHolder<String, DataSegment>> timeline = createTimeline();
    final Iterator<DruidSegmentInputEntity> entityIterator = FluentIterable
        .from(timeline)
        .transformAndConcat(holder -> {
          //noinspection ConstantConditions
          final PartitionHolder<DataSegment> partitionHolder = holder.getObject();
          //noinspection ConstantConditions
          return FluentIterable
              .from(partitionHolder)
              .transform(chunk -> new DruidSegmentInputEntity(segmentCacheManager, chunk.getObject(), holder.getInterval()));
        }).iterator();

    final DruidSegmentInputFormat inputFormat = new DruidSegmentInputFormat(indexIO, dimFilter);

    return new InputEntityIteratingReader(
        getInputRowSchemaToUse(inputRowSchema),
        inputFormat,
        CloseableIterators.withEmptyBaggage(entityIterator),
        SystemFieldDecoratorFactory.NONE,
        temporaryDirectory
    );
  }

  @VisibleForTesting
  InputRowSchema getInputRowSchemaToUse(InputRowSchema inputRowSchema)
  {
    final InputRowSchema inputRowSchemaToUse;

    ColumnsFilter columnsFilterToUse = inputRowSchema.getColumnsFilter();
    if (inputRowSchema.getMetricNames() != null) {
      for (String metricName : inputRowSchema.getMetricNames()) {
        columnsFilterToUse = columnsFilterToUse.plus(metricName);
      }
    }

    if (taskConfig.isIgnoreTimestampSpecForDruidInputSource()) {
      // Legacy compatibility mode; see https://github.com/apache/druid/pull/10267.
      LOG.warn("Ignoring the provided timestampSpec and reading the __time column instead. To use timestampSpecs with "
               + "the 'druid' input source, set druid.indexer.task.ignoreTimestampSpecForDruidInputSource to false.");

      inputRowSchemaToUse = new InputRowSchema(
          new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, STANDARD_TIME_COLUMN_FORMATS.iterator().next(), null),
          inputRowSchema.getDimensionsSpec(),
          columnsFilterToUse.plus(ColumnHolder.TIME_COLUMN_NAME)
      );
    } else {
      inputRowSchemaToUse = new InputRowSchema(
          inputRowSchema.getTimestampSpec(),
          inputRowSchema.getDimensionsSpec(),
          columnsFilterToUse
      );
    }

    if (ColumnHolder.TIME_COLUMN_NAME.equals(inputRowSchemaToUse.getTimestampSpec().getTimestampColumn())
        && !STANDARD_TIME_COLUMN_FORMATS.contains(inputRowSchemaToUse.getTimestampSpec().getTimestampFormat())) {
      // Slight chance the user did this intentionally, but not likely. Log a warning.
      LOG.warn(
          "The provided timestampSpec refers to the %s column without using format %s. If you wanted to read the "
          + "column as-is, switch formats.",
          inputRowSchemaToUse.getTimestampSpec().getTimestampColumn(),
          STANDARD_TIME_COLUMN_FORMATS
      );
    }

    return inputRowSchemaToUse;
  }

  private List<TimelineObjectHolder<String, DataSegment>> createTimeline()
  {
    if (interval == null) {
      return getTimelineForSegmentIds(coordinatorClient, dataSource, segmentIds);
    } else {
      return getTimelineForInterval(toolbox, coordinatorClient, dataSource, interval);
    }
  }

  @Override
  public Stream<InputSplit<List<WindowedSegmentId>>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    // segmentIds is supposed to be specified by the supervisor task during the parallel indexing.
    // If it's not null, segments are already split by the supervisor task and further split won't happen.
    if (segmentIds == null) {
      return Streams.sequentialStreamFrom(
          createSplits(
              toolbox,
              coordinatorClient,
              dataSource,
              interval,
              splitHintSpec == null ? SplittableInputSource.DEFAULT_SPLIT_HINT_SPEC : splitHintSpec
          )
      );
    } else {
      return Stream.of(new InputSplit<>(segmentIds));
    }
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    // segmentIds is supposed to be specified by the supervisor task during the parallel indexing.
    // If it's not null, segments are already split by the supervisor task and further split won't happen.
    if (segmentIds == null) {
      return Iterators.size(
          createSplits(
              toolbox,
              coordinatorClient,
              dataSource,
              interval,
              splitHintSpec == null ? SplittableInputSource.DEFAULT_SPLIT_HINT_SPEC : splitHintSpec
          )
      );
    } else {
      return 1;
    }
  }

  @Override
  public SplittableInputSource<List<WindowedSegmentId>> withSplit(InputSplit<List<WindowedSegmentId>> split)
  {
    return new DruidInputSource(
        dataSource,
        null,
        split.get(),
        dimFilter,
        dimensions,
        metrics,
        indexIO,
        coordinatorClient,
        segmentCacheManagerFactory,
        taskConfig,
        toolbox
    );
  }

  @Override
  public boolean needsFormat()
  {
    return false;
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
    DruidInputSource that = (DruidInputSource) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(interval, that.interval)
           && Objects.equals(segmentIds, that.segmentIds)
           && Objects.equals(dimFilter, that.dimFilter)
           && Objects.equals(dimensions, that.dimensions)
           && Objects.equals(metrics, that.metrics);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, interval, segmentIds, dimFilter, dimensions, metrics);
  }

  @Override
  public String toString()
  {
    return "DruidInputSource{" +
           "dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", segmentIds=" + segmentIds +
           ", dimFilter=" + dimFilter +
           (dimensions != null ? ", dimensions=" + dimensions : "") +
           (metrics != null ? ", metrics=" + metrics : "") +
           '}';
  }

  public static Iterator<InputSplit<List<WindowedSegmentId>>> createSplits(
      TaskToolbox toolbox,
      CoordinatorClient coordinatorClient,
      String dataSource,
      Interval interval,
      SplitHintSpec splitHintSpec
  )
  {
    final SplitHintSpec convertedSplitHintSpec;
    if (splitHintSpec instanceof SegmentsSplitHintSpec) {
      final SegmentsSplitHintSpec segmentsSplitHintSpec = (SegmentsSplitHintSpec) splitHintSpec;
      convertedSplitHintSpec = new MaxSizeSplitHintSpec(
          segmentsSplitHintSpec.getMaxInputSegmentBytesPerTask(),
          segmentsSplitHintSpec.getMaxNumSegments()
      );
    } else {
      convertedSplitHintSpec = splitHintSpec;
    }

    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = getTimelineForInterval(
        toolbox,
        coordinatorClient,
        dataSource,
        interval
    );
    final Map<WindowedSegmentId, Long> segmentIdToSize = createWindowedSegmentIdFromTimeline(timelineSegments);
    //noinspection ConstantConditions
    return Iterators.transform(
        convertedSplitHintSpec.split(
            // segmentIdToSize is sorted by segment ID; useful for grouping up segments from the same time chunk into
            // the same input split.
            segmentIdToSize.keySet().iterator(),
            segmentId -> new InputFileAttribute(
                Preconditions.checkNotNull(segmentIdToSize.get(segmentId), "segment size for [%s]", segmentId)
            )
        ),
        InputSplit::new
    );
  }

  /**
   * Returns a map of {@link WindowedSegmentId} to size, sorted by {@link WindowedSegmentId#getSegmentId()}.
   */
  private static SortedMap<WindowedSegmentId, Long> createWindowedSegmentIdFromTimeline(
      List<TimelineObjectHolder<String, DataSegment>> timelineHolders
  )
  {
    Map<DataSegment, WindowedSegmentId> windowedSegmentIds = new HashMap<>();
    for (TimelineObjectHolder<String, DataSegment> holder : timelineHolders) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        windowedSegmentIds.computeIfAbsent(
            chunk.getObject(),
            segment -> new WindowedSegmentId(segment.getId().toString(), new ArrayList<>())
        ).addInterval(holder.getInterval());
      }
    }
    // It is important to create this map after windowedSegmentIds is completely filled, because WindowedSegmentIds
    // can be updated while being constructed. (Intervals are added.)
    SortedMap<WindowedSegmentId, Long> segmentSizeMap = new TreeMap<>(WINDOWED_SEGMENT_ID_COMPARATOR);
    windowedSegmentIds.forEach((segment, segmentId) -> segmentSizeMap.put(segmentId, segment.getSize()));
    return segmentSizeMap;
  }

  public static List<TimelineObjectHolder<String, DataSegment>> getTimelineForInterval(
      TaskToolbox toolbox,
      CoordinatorClient coordinatorClient,
      String dataSource,
      Interval interval
  )
  {
    Preconditions.checkNotNull(interval);

    final Collection<DataSegment> usedSegments;
    if (toolbox == null) {
      usedSegments = FutureUtils.getUnchecked(
          coordinatorClient.fetchUsedSegments(dataSource, Collections.singletonList(interval)),
          true
      );
    } else {
      try {
        usedSegments = toolbox.getTaskActionClient()
                              .submit(new RetrieveUsedSegmentsAction(
                                  dataSource,
                                  Collections.singletonList(interval)
                              ));
      }
      catch (IOException e) {
        LOG.error(e, "Error retrieving the used segments for interval[%s].", interval);
        throw new RuntimeException(e);
      }
    }

    return SegmentTimeline.forSegments(usedSegments).lookup(interval);
  }

  public static List<TimelineObjectHolder<String, DataSegment>> getTimelineForSegmentIds(
      CoordinatorClient coordinatorClient,
      String dataSource,
      List<WindowedSegmentId> segmentIds
  )
  {
    final SortedMap<Interval, TimelineObjectHolder<String, DataSegment>> timeline = new TreeMap<>(
        Comparators.intervalsByStartThenEnd()
    );
    for (WindowedSegmentId windowedSegmentId : Preconditions.checkNotNull(segmentIds, "segmentIds")) {
      final DataSegment segment = FutureUtils.getUnchecked(
          coordinatorClient.fetchSegment(dataSource, windowedSegmentId.getSegmentId(), false),
          true
      );
      for (Interval interval : windowedSegmentId.getIntervals()) {
        final TimelineObjectHolder<String, DataSegment> existingHolder = timeline.get(interval);
        if (existingHolder != null) {
          if (!existingHolder.getVersion().equals(segment.getVersion())) {
            throw new ISE("Timeline segments with the same interval should have the same version: " +
                          "existing version[%s] vs new segment[%s]", existingHolder.getVersion(), segment);
          }
          existingHolder.getObject().add(segment.getShardSpec().createChunk(segment));
        } else {
          timeline.put(
              interval,
              new TimelineObjectHolder<>(
                  interval,
                  segment.getInterval(),
                  segment.getVersion(),
                  new PartitionHolder<>(segment.getShardSpec().createChunk(segment))
              )
          );
        }
      }
    }

    // Validate that none of the given windows overlaps (except for when multiple segments share exactly the
    // same interval).
    Interval lastInterval = null;
    for (Interval interval : timeline.keySet()) {
      if (lastInterval != null && interval.overlaps(lastInterval)) {
        throw new IAE(
            "Distinct intervals in input segments may not overlap: [%s] vs [%s]",
            lastInterval,
            interval
        );
      }
      lastInterval = interval;
    }

    return new ArrayList<>(timeline.values());
  }
}
