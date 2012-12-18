/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.metadata;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.SimpleSequence;
import com.metamx.druid.BaseStorageAdapter;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.v1.SegmentIdAttachedStorageAdapter;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SegmentMetadataResultValue;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;



public class SegmentMetadataQueryEngine
{
  public Sequence<Result<SegmentMetadataResultValue>> process(
      final SegmentMetadataQuery query,
      StorageAdapter storageAdapter
  )
  {
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    if(!(storageAdapter instanceof SegmentIdAttachedStorageAdapter) ||
       !(((SegmentIdAttachedStorageAdapter)storageAdapter).getDelegate() instanceof BaseStorageAdapter)) {
      return Sequences.empty();
    }

    final BaseStorageAdapter adapter = (BaseStorageAdapter)
        ((SegmentIdAttachedStorageAdapter) storageAdapter).getDelegate();

    Function<String, SegmentMetadataResultValue.Dimension> sizeDimension = new Function<String, SegmentMetadataResultValue.Dimension>()
    {
      @Override
      public SegmentMetadataResultValue.Dimension apply(@Nullable String input)
      {
        long size = 0;
        final Indexed<String> lookup = adapter.getDimValueLookup(input);
        for (String dimVal : lookup) {
          ImmutableConciseSet index = adapter.getInvertedIndex(input, dimVal);
          size += (dimVal == null) ? 0 : index.size() * Charsets.UTF_8.encode(dimVal).capacity();
        }
        return new SegmentMetadataResultValue.Dimension(
            size,
            adapter.getDimensionCardinality(input)
        );
      }
    };

    // TODO: add metric storage size

    long totalSize = 0;

    HashMap<String, SegmentMetadataResultValue.Dimension> dimensions = Maps.newHashMap();
    for(String input : adapter.getAvailableDimensions()) {
      SegmentMetadataResultValue.Dimension d = sizeDimension.apply(input);
      dimensions.put(input, d);
      totalSize += d.size;
    }

    return new SimpleSequence<Result<SegmentMetadataResultValue>>(
        ImmutableList.of(
            new Result<SegmentMetadataResultValue>(
                adapter.getMinTime(),
                new SegmentMetadataResultValue(
                    storageAdapter.getSegmentIdentifier(),
                    dimensions,
                    ImmutableMap.<String, SegmentMetadataResultValue.Metric>of(),
                    totalSize
                )
            )
        )
    );
  }
}