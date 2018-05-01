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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.indexing.common.TaskToolbox;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.guava.Comparators;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.RowFilteringIndexAdapter;
import io.druid.segment.RowPointer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 */
public class AppendTask extends MergeTaskBase
{
  private final IndexSpec indexSpec;
  private final List<AggregatorFactory> aggregators;

  @JsonCreator
  public AppendTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(id, dataSource, segments, segmentWriteOutMediumFactory, context);
    this.indexSpec = indexSpec == null ? new IndexSpec() : indexSpec;
    this.aggregators = aggregators;
  }

  @Override
  public File merge(final TaskToolbox toolbox, final Map<DataSegment, File> segments, final File outDir)
      throws Exception
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<String, DataSegment>(
        Comparators.naturalNullsFirst()
    );

    for (DataSegment segment : segments.keySet()) {
      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
    }

    final Iterable<SegmentToMergeHolder> segmentsToMerge = Iterables.concat(
        Iterables.transform(
            timeline.lookup(Intervals.of("1000-01-01/3000-01-01")),
            new Function<TimelineObjectHolder<String, DataSegment>, Iterable<SegmentToMergeHolder>>()
            {
              @Override
              public Iterable<SegmentToMergeHolder> apply(final TimelineObjectHolder<String, DataSegment> input)
              {
                return Iterables.transform(
                    input.getObject(),
                    new Function<PartitionChunk<DataSegment>, SegmentToMergeHolder>()
                    {
                      @Nullable
                      @Override
                      public SegmentToMergeHolder apply(PartitionChunk<DataSegment> chunkInput)
                      {
                        DataSegment segment = chunkInput.getObject();
                        return new SegmentToMergeHolder(
                            input.getInterval(),
                            Preconditions.checkNotNull(
                                segments.get(segment),
                                "File for segment %s", segment.getIdentifier()
                            )
                        );
                      }
                    }
                );
              }
            }
        )
    );

    List<IndexableAdapter> adapters = Lists.newArrayList();
    for (final SegmentToMergeHolder holder : segmentsToMerge) {
      adapters.add(
          new RowFilteringIndexAdapter(
              new QueryableIndexIndexableAdapter(toolbox.getIndexIO().loadIndex(holder.getFile())),
              (RowPointer rowPointer) -> holder.getInterval().contains(rowPointer.getTimestamp())
          )
      );
    }

    IndexMerger indexMerger = toolbox.getIndexMergerV9();
    return indexMerger.append(
        adapters,
        aggregators == null ? null : aggregators.toArray(new AggregatorFactory[aggregators.size()]),
        outDir,
        indexSpec,
        getSegmentWriteOutMediumFactory()
    );
  }

  @Override
  public String getType()
  {
    return "append";
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  private static class SegmentToMergeHolder
  {
    private final Interval interval;
    private final File file;

    private SegmentToMergeHolder(Interval interval, File file)
    {
      this.interval = interval;
      this.file = file;
    }

    public Interval getInterval()
    {
      return interval;
    }

    public File getFile()
    {
      return file;
    }
  }
}
