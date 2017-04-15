/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.epinephelinae;

import java.util.Objects;

public class AggregateResult
{
  private static final AggregateResult OK = new AggregateResult(true, null);

  private final boolean ok;
  private final String reason;

  public static AggregateResult ok()
  {
    return OK;
  }

  public static AggregateResult failure(final String reason)
  {
    return new AggregateResult(false, reason);
  }

  private AggregateResult(final boolean ok, final String reason)
  {
    this.ok = ok;
    this.reason = reason;
  }

  public boolean isOk()
  {
    return ok;
  }

  public String getReason()
  {
    return reason;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AggregateResult that = (AggregateResult) o;
    return ok == that.ok &&
           Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ok, reason);
  }

  @Override
  public String toString()
  {
    return "AggregateResult{" +
           "ok=" + ok +
           ", reason='" + reason + '\'' +
           '}';
  }
}
