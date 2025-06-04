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

import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;

/**
 * A {@link Segment} with an associated references, such as {@link ReferenceCountingSegment} where the reference is
 * the segment itself, and {@link org.apache.druid.segment.join.HashJoinSegment} which wraps a
 * {@link ReferenceCountingSegment} and also includes the associated list of
 * {@link org.apache.druid.segment.join.JoinableClause}
 */
public interface SegmentReference extends Segment, ReferenceCountedObject
{

  /**
   * Validates if the segment complies with the policy restrictions on tables.
   * <p>
   * This should be called right before the segment is about to be processed by the query stack, and after
   * {@link org.apache.druid.query.planning.ExecutionVertex#createSegmentMapFunction(PolicyEnforcer)}.
   */
  default void validateOrElseThrow(PolicyEnforcer policyEnforcer)
  {
    // For testing purposes, we allow the NoopPolicyEnforcer to pass through.
    if (policyEnforcer instanceof NoopPolicyEnforcer) {
      return;
    }
    throw new UnsupportedOperationException("validateOrElseThrow is not supported");
  }

}
