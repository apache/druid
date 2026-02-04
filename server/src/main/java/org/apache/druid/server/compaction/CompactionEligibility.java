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

import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

/**
 * Describes the eligibility of an interval for compaction.
 */
public class CompactionEligibility
{
  public enum State
  {
    FULL_COMPACTION,
    INCREMENTAL_COMPACTION,
    NOT_ELIGIBLE,
    NOT_APPLICABLE
  }

  public static final CompactionEligibility FULL_COMPACTION_ELIGIBLE = new CompactionEligibility(State.FULL_COMPACTION, null);
  public static final CompactionEligibility NOT_APPLICABLE = new CompactionEligibility(State.NOT_APPLICABLE, null);

  private final State state;
  private final String reason;

  private CompactionEligibility(State state, String reason)
  {
    this.state = state;
    this.reason = reason;
  }

  public State getState()
  {
    return state;
  }

  public String getReason()
  {
    return reason;
  }

  public static CompactionEligibility incrementalCompaction(String messageFormat, Object... args)
  {
    return new CompactionEligibility(State.INCREMENTAL_COMPACTION, StringUtils.format(messageFormat, args));
  }

  public static CompactionEligibility fail(String messageFormat, Object... args)
  {
    return new CompactionEligibility(State.NOT_ELIGIBLE, StringUtils.format(messageFormat, args));
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
    CompactionEligibility that = (CompactionEligibility) object;
    return state == that.state && Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(state, reason);
  }

  @Override
  public String toString()
  {
    return "Eligibility{" +
           "state=" + state +
           ", reason='" + reason + '\'' +
           '}';
  }
}
