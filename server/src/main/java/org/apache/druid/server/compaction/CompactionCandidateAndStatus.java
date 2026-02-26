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

import java.util.Objects;

/**
 * Wrapper class that combines a compaction candidate with its evaluated status.
 */
public class CompactionCandidateAndStatus
{
  private final CompactionCandidate candidate;
  private final CompactionStatus status;

  public CompactionCandidateAndStatus(
      CompactionCandidate candidate,
      CompactionStatus status
  )
  {
    this.candidate = candidate;
    this.status = status;
  }

  public CompactionCandidate getCandidate()
  {
    return candidate;
  }

  public CompactionStatus getStatus()
  {
    return status;
  }

  public String getDataSource()
  {
    return candidate.getDataSource();
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    CompactionCandidateAndStatus that = (CompactionCandidateAndStatus) object;
    return Objects.equals(candidate, that.candidate)
           && Objects.equals(status, that.status);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(candidate, status);
  }

  @Override
  public String toString()
  {
    return "CompactionCandidateAndStatus{"
           + "candidate=" + candidate
           + ", status=" + status
           + '}';
  }
}
