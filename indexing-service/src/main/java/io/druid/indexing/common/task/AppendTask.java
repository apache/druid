/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.RowboatFilteringIndexAdapter;
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
  @JsonCreator
  public AppendTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments
  )
  {
    super(id, dataSource, segments);
  }

  @Override
  public File merge(final Map<DataSegment, File> segments, final File outDir)
      throws Exception
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<String, DataSegment>(
        Ordering.<String>natural().nullsFirst()
    );

    for (DataSegment segment : segments.keySet()) {
      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
    }

    final Iterable<SegmentToMergeHolder> segmentsToMerge = Iterables.concat(
        Iterables.transform(
            timeline.lookup(new Interval("1000-01-01/3000-01-01")),
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
                            segment, input.getInterval(),
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
          new RowboatFilteringIndexAdapter(
              new QueryableIndexIndexableAdapter(
                  IndexIO.loadIndex(holder.getFile())
              ),
              new Predicate<Rowboat>()
              {
                @Override
                public boolean apply(Rowboat input)
                {
                  return holder.getInterval().contains(input.getTimestamp());
                }
              }
          )
      );
    }

    return IndexMerger.append(adapters, outDir);
  }

  @Override
  public String getType()
  {
    return "append";
  }

  private static class SegmentToMergeHolder
  {
    private final DataSegment segment;
    private final Interval interval;
    private final File file;

    private SegmentToMergeHolder(DataSegment segment, Interval interval, File file)
    {
      this.segment = segment;
      this.interval = interval;
      this.file = file;
    }

    public DataSegment getSegment()
    {
      return segment;
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
