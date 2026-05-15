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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Describes the eligibility of an interval for compaction.
 */
public class Eligibility
{
  /**
   * Denotes that the candidate interval is eligible for a
   * {@link CompactionMode#ALL_SEGMENTS full} compaction.
   */
  public static final Eligibility FULL = new Eligibility(true, null, CompactionMode.ALL_SEGMENTS);

  private final boolean eligible;
  private final String reason;
  @Nullable
  private final CompactionMode mode;

  private Eligibility(boolean eligible, String reason, @Nullable CompactionMode mode)
  {
    this.eligible = eligible;
    this.reason = reason;
    this.mode = mode;
  }

  public boolean isEligible()
  {
    return eligible;
  }

  public String getReason()
  {
    return reason;
  }

  /**
   * The mode of compaction (full or minor). This is non-null only when the
   * candidate is considered to be eligible for compaction by the policy.
   *
   * @throws DruidException if {@link #isEligible()} returns false.
   */
  public CompactionMode getMode()
  {
    if (!isEligible()) {
      throw DruidException.defensive("Cannot get mode since interval is not eligible for compaction");
    }
    return mode;
  }

  /**
   * Denotes that the candidate interval is not eligible for compaction.
   */
  public static Eligibility fail(String messageFormat, Object... args)
  {
    return new Eligibility(false, StringUtils.format(messageFormat, args), null);
  }

  /**
   * @return {@code Eligibility} denoting that the candidate interval is
   * eligible for a {@link CompactionMode#UNCOMPACTED_SEGMENTS_ONLY minor}
   * compaction.
   */
  public static Eligibility minor(String messageFormat, Object... args)
  {
    return new Eligibility(true, StringUtils.format(messageFormat, args), CompactionMode.UNCOMPACTED_SEGMENTS_ONLY);
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
    Eligibility that = (Eligibility) object;
    return eligible == that.eligible && Objects.equals(reason, that.reason) && Objects.equals(mode, that.mode);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(eligible, reason, mode);
  }

  @Override
  public String toString()
  {
    return "Eligibility{" +
           "eligible=" + eligible +
           ", reason='" + reason +
           ", mode='" + mode + '\'' +
           '}';
  }
}
