/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.result;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class BySegmentResultValueClass<T>
{
  private final List<T> results;
  private final String segmentId;
  private final String intervalString;

  public BySegmentResultValueClass(
      @JsonProperty("results") List<T> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") String intervalString
  )
  {
    this.results = results;
    this.segmentId = segmentId;
    this.intervalString = intervalString;
  }

  @JsonProperty("results")
  public List<T> getResults()
  {
    return results;
  }

  @JsonProperty("segment")
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty("interval")
  public String getIntervalString()
  {
    return intervalString;
  }

  @Override
  public String toString()
  {
    return "BySegmentTimeseriesResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", intervalString='" + intervalString + '\'' +
           '}';
  }
}
