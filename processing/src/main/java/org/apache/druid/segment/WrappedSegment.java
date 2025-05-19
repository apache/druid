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

package org.apache.druid.segment;

import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.IOException;

/**
 * Simple {@link Segment} implementation for a segment that wraps a base segment such as
 * {@link UnnestSegment} or {@link FilteredSegment}
 */
public abstract class WrappedSegment implements Segment
{
  protected final Segment delegate;

  public WrappedSegment(
      Segment delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public SegmentId getId()
  {
    return delegate.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return delegate.getDataInterval();
  }

  @Override
  public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
  {
    delegate.validateOrElseThrow(policyEnforcer);
  }

  @Override
  public boolean isTombstone()
  {
    return delegate.isTombstone();
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}

