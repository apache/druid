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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.granularity.BaseGranularitySpec;

import java.util.Objects;

public class ClientCompactionTaskQueryGranularitySpec
{
  private final Granularity segmentGranularity;
  private final Granularity queryGranularity;
  private final boolean rollup;

  @JsonCreator
  public ClientCompactionTaskQueryGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup
  )
  {
    this.queryGranularity = queryGranularity == null ? BaseGranularitySpec.DEFAULT_QUERY_GRANULARITY : queryGranularity;
    this.rollup = rollup == null ? BaseGranularitySpec.DEFAULT_ROLLUP : rollup;
    this.segmentGranularity = segmentGranularity == null ? BaseGranularitySpec.DEFAULT_SEGMENT_GRANULARITY : segmentGranularity;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public boolean isRollup()
  {
    return rollup;
  }

  @Override
  public String toString()
  {
    return "ClientCompactionTaskQueryGranularitySpec{" +
           "segmentGranularity=" + segmentGranularity +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           '}';
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
    ClientCompactionTaskQueryGranularitySpec that = (ClientCompactionTaskQueryGranularitySpec) o;
    return Objects.equals(segmentGranularity, that.segmentGranularity) &&
           Objects.equals(queryGranularity, that.queryGranularity) &&
           Objects.equals(rollup, that.rollup);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentGranularity, queryGranularity, rollup);
  }
}
