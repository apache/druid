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

package org.apache.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularity;

import java.util.Objects;

public class CompactionGranularitySpec
{
  private final Granularity segmentGranularity;
  private final Granularity queryGranularity;

  @JsonCreator
  public CompactionGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") Granularity queryGranularity
  )
  {
    this.queryGranularity = queryGranularity;
    this.segmentGranularity = segmentGranularity;
  }

  @JsonProperty("segmentGranularity")
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty("queryGranularity")
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  public CompactionGranularitySpec withSegmentGranularity(Granularity segmentGranularity)
  {
    return new CompactionGranularitySpec(segmentGranularity, queryGranularity);
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
    CompactionGranularitySpec that = (CompactionGranularitySpec) o;
    return Objects.equals(segmentGranularity, that.segmentGranularity) &&
           Objects.equals(queryGranularity, that.queryGranularity);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentGranularity, queryGranularity);
  }

  @Override
  public String toString()
  {
    return "CompactionGranularitySpec{" +
           "segmentGranularity=" + segmentGranularity +
           ", queryGranularity=" + queryGranularity +
           '}';
  }
}
