/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
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

    final List<SegmentToMergeHolder> segmentsToMerge = Lists.transform(
        timeline.lookup(new Interval("1000-01-01/3000-01-01")),
        new Function<TimelineObjectHolder<String, DataSegment>, SegmentToMergeHolder>()
        {
          @Override
          public SegmentToMergeHolder apply(TimelineObjectHolder<String, DataSegment> input)
          {
            final DataSegment segment = input.getObject().getChunk(0).getObject();
            final File file = Preconditions.checkNotNull(
                segments.get(segment),
                "File for segment %s", segment.getIdentifier()
            );

            return new SegmentToMergeHolder(segment, input.getInterval(), file);
          }
        }
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
