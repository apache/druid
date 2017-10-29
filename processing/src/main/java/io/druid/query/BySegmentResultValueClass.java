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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 */
public class BySegmentResultValueClass<T> implements BySegmentResultValue<T>
{
  private final List<T> results;
  private final String segmentId;
  private final Interval interval;

  public BySegmentResultValueClass(
      @JsonProperty("results") List<T> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") Interval interval
  )
  {
    this.results = results;
    this.segmentId = segmentId;
    this.interval = interval;
  }

  @Override
  @JsonProperty("results")
  public List<T> getResults()
  {
    return results;
  }

  @Override
  @JsonProperty("segment")
  public String getSegmentId()
  {
    return segmentId;
  }

  @Override
  @JsonProperty("interval")
  public Interval getInterval()
  {
    return interval;
  }

  public <U> BySegmentResultValueClass<U> mapResults(Function<? super T, ? extends U> mapper)
  {
    List<U> mappedResults = results.stream().map(mapper).collect(Collectors.toList());
    return new BySegmentResultValueClass<>(mappedResults, segmentId, interval);
  }

  @Override
  public String toString()
  {
    return "BySegmentResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", interval='" + interval + '\'' +
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

    BySegmentResultValueClass that = (BySegmentResultValueClass) o;

    if (interval != null ? !interval.equals(that.interval) : that.interval != null) {
      return false;
    }
    if (results != null ? !results.equals(that.results) : that.results != null) {
      return false;
    }
    if (segmentId != null ? !segmentId.equals(that.segmentId) : that.segmentId != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = results != null ? results.hashCode() : 0;
    result = 31 * result + (segmentId != null ? segmentId.hashCode() : 0);
    result = 31 * result + (interval != null ? interval.hashCode() : 0);
    return result;
  }
}
