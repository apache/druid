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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.server.coordinator.duty.CompactSegments;

import java.util.Comparator;

/**
 * Policy used by {@link CompactSegments} duty to pick segments for compaction.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "newestSegmentFirst", value = NewestSegmentFirstPolicy.class)
})
public interface CompactionCandidateSearchPolicy extends Comparator<CompactionCandidate>
{
  /**
   * Compares between two compaction candidates. Used to determine the
   * order in which segments and intervals should be picked for compaction.
   */
  @Override
  int compare(CompactionCandidate o1, CompactionCandidate o2);

  /**
   * Checks if the given {@link CompactionCandidate} is eligible for compaction
   * in the current iteration. A policy may implement this method to skip
   * compacting intervals or segments that do not fulfil some required criteria.
   */
  boolean isEligibleForCompaction(
      CompactionCandidate candidate,
      CompactionStatus currentCompactionStatus,
      CompactionTaskStatus latestTaskStatus
  );
}
