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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

public class FilterTuning
{
  public static FilterTuning createDefault(boolean useIndex)
  {
    return new FilterTuning(useIndex, 0, Integer.MAX_VALUE);
  }

  private final Boolean useIndex;
  private final Integer useIndexMinCardinalityThreshold;
  private final Integer useIndexMaxCardinalityThreshold;

  @JsonCreator
  public FilterTuning(
      @Nullable @JsonProperty("useIndex") Boolean useIndex,
      @Nullable @JsonProperty("useIndexMinCardinalityThreshold") Integer useIndexMinCardinalityThreshold,
      @Nullable @JsonProperty("useIndexMaximumCardinalityThreshold") Integer useIndexMaxCardinalityThreshold
  )
  {
    this.useIndex = useIndex;
    this.useIndexMinCardinalityThreshold = useIndexMinCardinalityThreshold;
    this.useIndexMaxCardinalityThreshold = useIndexMaxCardinalityThreshold;
  }

  @JsonProperty
  public Boolean getUseIndex()
  {
    return useIndex != null ? useIndex : true;
  }

  @JsonProperty
  public Integer getUseIndexMinCardinalityThreshold()
  {
    return useIndexMinCardinalityThreshold != null ? useIndexMinCardinalityThreshold : 0;
  }

  @JsonProperty
  public Integer getUseIndexMaxCardinalityThreshold()
  {
    return useIndexMaxCardinalityThreshold != null ? useIndexMaxCardinalityThreshold : Integer.MAX_VALUE;
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
    FilterTuning that = (FilterTuning) o;
    return Objects.equals(useIndex, that.useIndex) &&
           Objects.equals(useIndexMinCardinalityThreshold, that.useIndexMinCardinalityThreshold) &&
           Objects.equals(useIndexMaxCardinalityThreshold, that.useIndexMaxCardinalityThreshold);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(useIndex, useIndexMinCardinalityThreshold, useIndexMaxCardinalityThreshold);
  }

  @Override
  public String toString()
  {
    return "FilterTuning{" +
           "useIndex=" + useIndex +
           ", useIndexMinCardinalityThreshold=" + useIndexMinCardinalityThreshold +
           ", useIndexMaxCardinalityThreshold=" + useIndexMaxCardinalityThreshold +
           '}';
  }
}
