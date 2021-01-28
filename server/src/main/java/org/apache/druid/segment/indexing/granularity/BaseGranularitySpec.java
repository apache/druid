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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.indexing.LookupIntervalBuckets;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

abstract class BaseGranularitySpec implements GranularitySpec
{
  protected List<Interval> inputIntervals;
  protected final Boolean rollup;

  public BaseGranularitySpec(List<Interval> inputIntervals, Boolean rollup)
  {
    if (inputIntervals != null) {
      this.inputIntervals = ImmutableList.copyOf(inputIntervals);
    } else {
      this.inputIntervals = Collections.emptyList();
    }
    this.rollup = rollup == null ? Boolean.TRUE : rollup;
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> inputIntervals()
  {
    return inputIntervals;
  }

  @Override
  @JsonProperty("rollup")
  public boolean isRollup()
  {
    return rollup;
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return getLookupTableBuckets().bucketInterval(dt);
  }

  @Override
  public TreeSet<Interval> materializedBucketIntervals() { return getLookupTableBuckets().materializedIntervals(); }

  protected abstract LookupIntervalBuckets getLookupTableBuckets();

}
