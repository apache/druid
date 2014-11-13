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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 */
public class MergeTask extends MergeTaskBase
{
  @JsonIgnore
  private final List<AggregatorFactory> aggregators;

  @JsonCreator
  public MergeTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators
  )
  {
    super(id, dataSource, segments);
    this.aggregators = aggregators;
  }

  @Override
  public File merge(final Map<DataSegment, File> segments, final File outDir)
      throws Exception
  {
    return IndexMerger.mergeQueryableIndex(
        Lists.transform(
            ImmutableList.copyOf(segments.values()),
            new Function<File, QueryableIndex>()
            {
              @Override
              public QueryableIndex apply(@Nullable File input)
              {
                try {
                  return IndexIO.loadIndex(input);
                }
                catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
        ),
        aggregators.toArray(new AggregatorFactory[aggregators.size()]),
        outDir
    );
  }

  @Override
  public String getType()
  {
    return "merge";
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }
}
