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
package io.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ScanResultValue implements Comparable<ScanResultValue>
{
  private final String segmentId;
  private final List<String> columns;
  private final Object events;

  @JsonCreator
  public ScanResultValue(
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("events") Object events
  )
  {
    this.segmentId = segmentId;
    this.columns = columns;
    this.events = events;
  }

  @JsonProperty
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public Object getEvents()
  {
    return events;
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

    ScanResultValue that = (ScanResultValue) o;

    if (segmentId != null ? !segmentId.equals(that.segmentId) : that.segmentId != null) {
      return false;
    }
    if (columns != null ? !columns.equals(that.columns) : that.columns != null) {
      return false;
    }
    return events != null ? events.equals(that.events) : that.events == null;
  }

  @Override
  public int hashCode()
  {
    int result = segmentId != null ? segmentId.hashCode() : 0;
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    result = 31 * result + (events != null ? events.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ScanResultValue{" +
           "segmentId='" + segmentId + '\'' +
           ", columns=" + columns +
           ", events=" + events +
           '}';
  }

  @Override
  public int compareTo(ScanResultValue that)
  {
    if (that == null) {
      return 1;
    }
    if (segmentId != null && that.segmentId != null) {
      return segmentId.compareTo(that.segmentId);
    }
    return segmentId != null ? 1 : 0;
  }
}
