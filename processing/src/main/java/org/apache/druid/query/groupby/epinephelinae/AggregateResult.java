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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Objects;

public class AggregateResult
{
  private static final AggregateResult OK = new AggregateResult(0, null);

  private final int count;

  @Nullable
  private final String reason;

  public static AggregateResult ok()
  {
    return OK;
  }

  public static AggregateResult partial(final int count, final String reason)
  {
    return new AggregateResult(count, Preconditions.checkNotNull(reason, "reason"));
  }

  private AggregateResult(final int count, @Nullable final String reason)
  {
    Preconditions.checkArgument(count >= 0, "count >= 0");
    this.count = count;
    this.reason = reason;
  }

  /**
   * True if all rows have been processed.
   */
  public boolean isOk()
  {
    return reason == null;
  }

  public int getCount()
  {
    if (isOk()) {
      throw new ISE("Cannot call getCount when isOk = true");
    }

    return count;
  }

  @Nullable
  public String getReason()
  {
    if (isOk()) {
      throw new ISE("Cannot call getReason when isOk = true");
    }

    return reason;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AggregateResult that = (AggregateResult) o;
    return count == that.count &&
           Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(count, reason);
  }

  @Override
  public String toString()
  {
    return "AggregateResult{" +
           "count=" + count +
           ", reason='" + reason + '\'' +
           '}';
  }
}
