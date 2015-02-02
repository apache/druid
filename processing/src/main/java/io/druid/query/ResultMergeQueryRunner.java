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

import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.common.guava.CombiningSequence;

import java.util.Map;

/**
 */
public abstract class ResultMergeQueryRunner<T> extends BySegmentSkippingQueryRunner<T>
{
  public ResultMergeQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    super(baseRunner);
  }

  @Override
  public Sequence<T> doRun(QueryRunner<T> baseRunner, Query<T> query, Map<String, Object> context)
  {
    return CombiningSequence.create(baseRunner.run(query, context), makeOrdering(query), createMergeFn(query));
  }

  protected abstract Ordering<T> makeOrdering(Query<T> query);

  protected abstract BinaryFn<T,T,T> createMergeFn(Query<T> query);
}
