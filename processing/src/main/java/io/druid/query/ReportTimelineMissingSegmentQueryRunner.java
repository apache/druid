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

package io.druid.query;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;

import java.util.List;
import java.util.Map;

/**
 */
public class ReportTimelineMissingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final SegmentDescriptor descriptor;

  public ReportTimelineMissingSegmentQueryRunner(SegmentDescriptor descriptor)
  {
    this.descriptor = descriptor;
  }

  @Override
  public Sequence<T> run(
      Query<T> query, Map<String, Object> responseContext
  )
  {
    List<SegmentDescriptor> missingSegments = (List<SegmentDescriptor>) responseContext.get(Result.MISSING_SEGMENTS_KEY);
    if (missingSegments == null) {
      missingSegments = Lists.newArrayList();
      responseContext.put(Result.MISSING_SEGMENTS_KEY, missingSegments);
    }
    missingSegments.add(descriptor);
    return Sequences.empty();
  }
}
