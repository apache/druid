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
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

/**
 */
public class BySegmentQueryRunner<T> implements QueryRunner<T>
{
  private final String segmentIdentifier;
  private final DateTime timestamp;
  private final QueryRunner<T> base;

  public BySegmentQueryRunner(
      String segmentIdentifier,
      DateTime timestamp,
      QueryRunner<T> base
  )
  {
    this.segmentIdentifier = segmentIdentifier;
    this.timestamp = timestamp;
    this.base = base;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<T> run(final Query<T> query)
  {
    if (query.getContextBySegment(false)) {
      final Sequence<T> baseSequence = base.run(query);

      final List<T> results = Sequences.toList(baseSequence, Lists.<T>newArrayList());
      return Sequences.simple(
          Arrays.asList(
              (T) new Result<BySegmentResultValueClass<T>>(
                  timestamp,
                  new BySegmentResultValueClass<T>(
                      results,
                      segmentIdentifier,
                      query.getIntervals().get(0)
                  )
              )
          )
      );
    }

    return base.run(query);
  }
}
