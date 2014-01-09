/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.query.BySegmentResultValue;
import io.druid.query.Result;

import java.util.List;

/**
 */
public class BySegmentTopNResultValue extends TopNResultValue implements BySegmentResultValue<TopNResultValue>
{
  private final List<Result<TopNResultValue>> results;
  private final String segmentId;
  private final String intervalString;

  @JsonCreator
  public BySegmentTopNResultValue(
      @JsonProperty("results") List<Result<TopNResultValue>> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") String intervalString
  )
  {
    super(null);

    this.results = results;
    this.segmentId = segmentId;
    this.intervalString = intervalString;
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
  public String getIntervalString()
  {
    return intervalString;
  }

  @Override
  public String toString()
  {
    return "BySegmentTopNResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", intervalString='" + intervalString + '\'' +
           '}';
  }
}
