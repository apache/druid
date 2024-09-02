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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.guava.Comparators;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

/**
 * Base implementation of {@link CompactionCandidateSearchPolicy} that can have
 * a {@code priorityDatasource}.
 */
public abstract class BaseCandidateSearchPolicy
    implements CompactionCandidateSearchPolicy, Comparator<CompactionCandidate>
{
  private final String priorityDatasource;
  private final Comparator<CompactionCandidate> comparator;

  protected BaseCandidateSearchPolicy(@Nullable String priorityDatasource)
  {
    this.priorityDatasource = priorityDatasource;
    if (priorityDatasource == null || priorityDatasource.isEmpty()) {
      this.comparator = getSegmentComparator();
    } else {
      this.comparator = Comparators.alwaysFirst(priorityDatasource)
                                   .onResultOf(CompactionCandidate::getDataSource)
                                   .thenComparing(getSegmentComparator());
    }
  }

  /**
   * The candidates of this datasource are prioritized over all others.
   */
  @Nullable
  @JsonProperty
  public final String getPriorityDatasource()
  {
    return priorityDatasource;
  }

  @Override
  public final int compare(CompactionCandidate o1, CompactionCandidate o2)
  {
    return comparator.compare(o1, o2);
  }

  @Override
  public boolean isEligibleForCompaction(
      CompactionCandidate candidate,
      CompactionStatus currentCompactionStatus,
      CompactionTaskStatus latestTaskStatus
  )
  {
    return true;
  }

  /**
   * Compares between two compaction candidates. Used to determine the
   * order in which segments and intervals should be picked for compaction.
   */
  protected abstract Comparator<CompactionCandidate> getSegmentComparator();

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseCandidateSearchPolicy that = (BaseCandidateSearchPolicy) o;
    return Objects.equals(this.priorityDatasource, that.priorityDatasource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(priorityDatasource);
  }
}
