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

package org.apache.druid.msq.querykit;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.SegmentReference;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Processor that creates a segment mapping function that does *not* require broadcast join data. The resulting segment
 * mapping function embeds the joinable data within itself, and can be applied anywhere that would otherwise have used
 * {@link org.apache.druid.query.JoinDataSource#createSegmentMapFunction(Query, AtomicLong)}.
 *
 * @see BroadcastJoinSegmentMapFnProcessor processor that creates a segment mapping function when there is
 * broadcast input
 */
public class SimpleSegmentMapFnProcessor implements FrameProcessor<Function<SegmentReference, SegmentReference>>
{
  private final Query<?> query;
  private final PolicyEnforcer policyEnforcer;

  public SimpleSegmentMapFnProcessor(final Query<?> query,
                                     final PolicyEnforcer policyEnforcer)
  {
    this.query = query;
    this.policyEnforcer = policyEnforcer;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<Function<SegmentReference, SegmentReference>> runIncrementally(final IntSet readableInputs)
  {
    ExecutionVertex ev = ExecutionVertex.of(query);
    return ReturnOrAwait.returnObject(ev.createSegmentMapFunction(policyEnforcer));
  }

  @Override
  public void cleanup()
  {
    // Nothing to do.
  }
}
