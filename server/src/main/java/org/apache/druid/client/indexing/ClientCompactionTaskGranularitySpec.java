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
import org.apache.druid.segment.indexing.granularity.GranularitySpec;

import java.util.Objects;

/**
 * Spec containing Granularity configs for Compaction Task.
 * This class mimics JSON field names for fields supported in compaction task with
 * the corresponding fields in {@link GranularitySpec}.
 * This is done for end-user ease of use. Basically, end-user will use the same syntax / JSON structure to set
 * Granularity configs for Compaction task as they would for any other ingestion task.
 * Note that this class is not the same as {@link GranularitySpec}. This class simply holds Granularity configs
 * and use it to generate index task specs (Compaction task internally creates index task).
 * This class does not do bucketing, group events or knows how to partition data.
 */
public class ClientCompactionTaskGranularitySpec
{
  private final Granularity segmentGranularity;
  private final Granularity queryGranularity;

  @JsonCreator
  public ClientCompactionTaskGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") Granularity queryGranularity
  )
  {
    this.queryGranularity = queryGranularity;
    this.segmentGranularity = segmentGranularity;
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

  public ClientCompactionTaskGranularitySpec withSegmentGranularity(Granularity segmentGranularity)
  {
    return new ClientCompactionTaskGranularitySpec(segmentGranularity, queryGranularity);
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
    ClientCompactionTaskGranularitySpec that = (ClientCompactionTaskGranularitySpec) o;
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
    return "ClientCompactionTaskGranularitySpec{" +
           "segmentGranularity=" + segmentGranularity +
           ", queryGranularity=" + queryGranularity +
           '}';
  }
}
