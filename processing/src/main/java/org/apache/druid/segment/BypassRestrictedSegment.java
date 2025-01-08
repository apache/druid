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

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.policy.Policy;

/**
 * A wrapped {@link SegmentReference} with a {@link DimFilter} restriction, and the policy restriction can be bypassed.
 * <p>
 * In some methods, such as {@link #as(Class)}, {@link #asQueryableIndex()}, and {@link #asCursorFactory()}, the policy
 * is ignored.
 */
class BypassRestrictedSegment extends RestrictedSegment
{
  public BypassRestrictedSegment(SegmentReference delegate, Policy policy)
  {
    super(delegate, policy);
  }

  public Policy getPolicy()
  {
    return policy;
  }

  @Override
  public CursorFactory asCursorFactory()
  {
    return delegate.asCursorFactory();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return delegate.asQueryableIndex();
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    return delegate.as(clazz);
  }
}
