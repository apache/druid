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
import org.apache.druid.segment.ReferenceCountingSegment;

import java.util.Map;

/**
 */
public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunnerFactory<T, Query<T>> factory;
  private final ReferenceCountingSegment adapter;
  private final SegmentDescriptor descriptor;

  public ReferenceCountingSegmentQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      ReferenceCountingSegment adapter,
      SegmentDescriptor descriptor
  )
  {
    this.factory = factory;
    this.adapter = adapter;
    this.descriptor = descriptor;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, Map<String, Object> responseContext)
  {
    if (adapter.increment()) {
      try {
        final Sequence<T> baseSequence = factory.createRunner(adapter).run(queryPlus, responseContext);

        return Sequences.withBaggage(baseSequence, adapter.decrementOnceCloseable());
      }
      catch (Throwable t) {
        try {
          adapter.decrement();
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        throw t;
      }
    } else {
      // Segment was closed before we had a chance to increment the reference count
      return new ReportTimelineMissingSegmentQueryRunner<T>(descriptor).run(queryPlus, responseContext);
    }
  }
}
