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

package org.apache.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.Result;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class BySegmentTopNResultValue extends TopNResultValue implements BySegmentResultValue<Result<TopNResultValue>>
{
  private final List<Result<TopNResultValue>> results;
  private final String segmentId;
  private final Interval interval;

  @JsonCreator
  public BySegmentTopNResultValue(
      @JsonProperty("results") List<Result<TopNResultValue>> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") Interval interval
  )
  {
    super(null);

    this.results = results;
    this.segmentId = segmentId;
    this.interval = interval;
  }

  @Override
  @JsonValue(false)
  public List<DimensionAndMetricValueExtractor> getValue()
  {
    throw new UnsupportedOperationException();
  }


  @Override
  @JsonProperty("results")
  public List<Result<TopNResultValue>> getResults()
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

  @Override
  public String toString()
  {
    return "BySegmentTopNResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", interval='" + interval + '\'' +
           '}';
  }
}
