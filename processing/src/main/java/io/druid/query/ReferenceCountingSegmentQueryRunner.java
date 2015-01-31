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

import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import io.druid.segment.ReferenceCountingSegment;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
*/
public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunnerFactory<T, Query<T>> factory;
  private final ReferenceCountingSegment adapter;

  public ReferenceCountingSegmentQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      ReferenceCountingSegment adapter
  )
  {
    this.factory = factory;
    this.adapter = adapter;
  }

  @Override
  public Sequence<T> run(final Query<T> query, Map<String, Object> responseContext)
  {
    final Closeable closeable = adapter.increment();
    try {
      final Sequence<T> baseSequence = factory.createRunner(adapter).run(query, responseContext);

      return new ResourceClosingSequence<T>(baseSequence, closeable);
    }
    catch (RuntimeException e) {
      CloseQuietly.close(closeable);
      throw e;
    }
  }
}
