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

package org.apache.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 */
public class MultipleIntervalSegmentSpec implements QuerySegmentSpec
{
  private final List<Interval> intervals;

  @JsonCreator
  public MultipleIntervalSegmentSpec(
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "intervals=" + intervals +
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

    MultipleIntervalSegmentSpec that = (MultipleIntervalSegmentSpec) o;

    if (intervals != null ? !intervals.equals(that.intervals) : that.intervals != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return intervals != null ? intervals.hashCode() : 0;
  }
}
