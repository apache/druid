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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;

public class OffsetLimit
{
  protected final long offset;
  protected final long limit;

  public static final OffsetLimit NONE = new OffsetLimit(0, -1);

  @JsonCreator
  public OffsetLimit(
      @JsonProperty("offset") long offset,
      @JsonProperty("limit") long limit)
  {
    Preconditions.checkArgument(offset >= 0, "offset >= 0");
    this.offset = offset;
    this.limit = limit < 0 ? -1 : limit;
  }

  @JsonProperty("offset")
  public long getOffset()
  {
    return offset;
  }

  @JsonProperty("limit")
  public long getLimit()
  {
    return limit;
  }

  public boolean isPresent()
  {
    return hasOffset() || hasLimit();
  }

  public boolean hasOffset()
  {
    return offset > 0;
  }

  public boolean hasLimit()
  {
    return limit >= 0;
  }

  public static OffsetLimit limit(int limit2)
  {
    return new OffsetLimit(0, limit2);
  }

  public long getLimitOrMax()
  {
    if (limit < 0) {
      return Long.MAX_VALUE;
    } else {
      return limit;
    }
  }

  @Override
  public final boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OffsetLimit)) {
      return false;
    }
    OffsetLimit that = (OffsetLimit) o;
    return limit == that.limit && offset == that.offset;
  }

  @Override
  public final int hashCode()
  {
    return Objects.hash(limit, offset);
  }

  @Override
  public String toString()
  {
    return "OffsetLimit{" +
        "offset=" + offset +
        ", limit=" + limit +
        '}';
  }

  /**
   * Returns the first row index to fetch.
   *
   * @param maxIndex maximal index accessible
   */
  public long getFromIndex(long maxIndex)
  {
    if (maxIndex <= offset) {
      return 0;
    }
    return offset;
  }

  /**
   * Returns the last row index to fetch (non-inclusive).
   *
   * @param maxIndex maximal index accessible
   */
  public long getToIndex(long maxIndex)
  {
    if (maxIndex <= offset) {
      return 0;
    }
    if (hasLimit()) {
      long toIndex = limit + offset;
      return Math.min(maxIndex, toIndex);
    } else {
      return maxIndex;
    }
  }
}
