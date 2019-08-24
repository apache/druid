/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;

/**
 * Query runner that wraps a base single-segment query runner, and wraps its results in a
 * {@link BySegmentResultValueClass} object if the "bySegment" query context parameter is set. Otherwise, it
 * delegates to the base runner without any behavior modification.
 *
 * Note that despite the type parameter "T", this runner may not actually return sequences with type T. They
 * may really be of type {@code Result<BySegmentResultValue<T>>}, if "bySegment" is set. Downstream consumers
 * of the returned sequence must be aware of this, and can use {@link QueryContexts#isBySegment(Query)} to
 * know what to expect.
 */
public class BySegmentQueryRunner<T> implements QueryRunner<T>
{
  private final SegmentId segmentId;
  private final DateTime timestamp;
  private final QueryRunner<T> base;

  public BySegmentQueryRunner(SegmentId segmentId, DateTime timestamp, QueryRunner<T> base)
  {
    this.segmentId = segmentId;
    this.timestamp = timestamp;
    this.base = base;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<T> run(final QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    if (QueryContexts.isBySegment(queryPlus.getQuery())) {
      final Sequence<T> baseSequence = base.run(queryPlus, responseContext);
      final List<T> results = baseSequence.toList();
      return Sequences.simple(
          Collections.singletonList(
              (T) new Result<>(
                  timestamp,
                  new BySegmentResultValueClass<>(
                      results,
                      segmentId.toString(),
                      queryPlus.getQuery().getIntervals().get(0)
                  )
              )
          )
      );
    }
    return base.run(queryPlus, responseContext);
  }
}
