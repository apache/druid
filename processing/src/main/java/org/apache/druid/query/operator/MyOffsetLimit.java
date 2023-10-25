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

public class MyOffsetLimit
{
  protected final long offset;
  protected final long limit;

  @JsonCreator
  public MyOffsetLimit(long offset, long limit)
  {
    this.offset = offset;
    this.limit = limit;
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

  public static MyOffsetLimit limit(int limit2)
  {
    return new MyOffsetLimit(0, limit2);
  }

  @JsonProperty
  public long getOffset()
  {
    return offset;
  }

  @JsonProperty
  public long getLimit()
  {
    return limit;
  }

  public long getLimitOrMax()
  {
    if (limit < 0) {
      return Long.MAX_VALUE;
    } else {
      return limit;
    }
  }

  public static MyOffsetLimit none()
  {
    return new MyOffsetLimit(0, -1);
  }
}
