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

package io.druid.query;

import com.metamx.common.guava.Sequence;

import java.util.Map;

/**
 */
public abstract class BySegmentSkippingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public BySegmentSkippingQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
  {
    if (query.getContextBySegment(false)) {
      return baseRunner.run(query, responseContext);
    }

    return doRun(baseRunner, query, responseContext);
  }

  protected abstract Sequence<T> doRun(QueryRunner<T> baseRunner, Query<T> query, Map<String, Object> context);
}
