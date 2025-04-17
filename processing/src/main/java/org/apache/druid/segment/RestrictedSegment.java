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

import com.google.common.base.Preconditions;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * A {@link SegmentReference} wrapper with a {@link Policy} restriction that is automatically enforced.
 * The policy seamlessly governs queries on the wrapped segment, ensuring compliance. For example,
 * {@link #asCursorFactory()} returns a policy-enforced {@link RestrictedCursorFactory}.
 *
 * <p>
 * Direct access to the policy or the underlying SegmentReference (the delegate) is not allowed.
 * However, a backdoor is available via {@code as(BypassRestrictedSegment.class)}, allowing access to
 * a {@link BypassRestrictedSegment} instance, which provides flexibility on policy enforcement.
 */
public class RestrictedSegment implements SegmentReference
{
  protected final SegmentReference delegate;
  protected final Policy policy;

  public RestrictedSegment(SegmentReference delegate, Policy policy)
  {
    // This is a sanity check, a restricted data source should alway wrap a druid table directly.
    Preconditions.checkArgument(
        delegate instanceof ReferenceCountingSegment,
        "delegate must be a ReferenceCountingSegment"
    );
    this.delegate = delegate;
    this.policy = policy;
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return delegate.acquireReferences();
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
  public CursorFactory asCursorFactory()
  {
    return new RestrictedCursorFactory(delegate.asCursorFactory(), policy);
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) asCursorFactory();
    } else if (QueryableIndex.class.equals(clazz)) {
      return null;
    } else if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) WrappedTimeBoundaryInspector.create(delegate.as(TimeBoundaryInspector.class));
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      return (T) new SimpleTopNOptimizationInspector(policy instanceof NoRestrictionPolicy);
    } else if (BypassRestrictedSegment.class.equals(clazz)) {
      // A backdoor solution to get the wrapped segment, effectively bypassing the policy.
      return (T) new BypassRestrictedSegment(delegate, policy);
    }

    // Unless we know there's no restriction, it's dangerous to return the implementation of a particular interface.
    if (policy instanceof NoRestrictionPolicy) {
      return delegate.as(clazz);
    }
    return null;
  }

  @Override
  public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
  {
    policyEnforcer.validateOrElseThrow((ReferenceCountingSegment) delegate, policy);
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

  @Override
  public String asString()
  {
    return delegate.asString();
  }
}
