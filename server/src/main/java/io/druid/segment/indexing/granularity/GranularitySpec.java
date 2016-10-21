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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;

import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.Granularity;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.SortedSet;

/**
 * Tells the indexer how to group events based on timestamp. The events may then be further partitioned based
 *  on anything, using a ShardSpec.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = UniformGranularitySpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "uniform", value = UniformGranularitySpec.class),
    @JsonSubTypes.Type(name = "arbitrary", value = ArbitraryGranularitySpec.class)
})
public interface GranularitySpec
{
  /**
   * Set of all time groups, broken up on segment boundaries. Should be sorted by interval start and non-overlapping.
   *
   * @return set of all time groups
   */
   public Optional<SortedSet<Interval>> bucketIntervals();

  /**
   * Time-grouping interval corresponding to some instant, if any.
   *
   * @param dt instant to return time interval for
   * @return optional time interval
   * */
  public Optional<Interval> bucketInterval(DateTime dt);

  public Granularity getSegmentGranularity();

  public boolean isRollup();

  public QueryGranularity getQueryGranularity();

  public String getTimezone();
}
