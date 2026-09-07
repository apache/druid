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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class TDigestConfig
{
  @JsonProperty("maxCompression")
  @Nullable
  private final Integer maxCompression;

  @JsonCreator
  public TDigestConfig(@JsonProperty("maxCompression") @Nullable Integer maxCompression)
  {
    this.maxCompression = maxCompression;
  }

  @Nullable
  public Integer getMaxCompression()
  {
    return maxCompression;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    @Nullable
    private Integer maxCompression;

    public Builder maxCompression(@Nullable Integer maxCompression)
    {
      this.maxCompression = maxCompression;
      return this;
    }

    public TDigestConfig build()
    {
      return new TDigestConfig(maxCompression);
    }
  }
}
