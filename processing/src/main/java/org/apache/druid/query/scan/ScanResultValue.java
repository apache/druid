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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ScanResultValue implements Comparable<ScanResultValue>
{
  /**
   * Segment id is stored as a String rather than {@link org.apache.druid.timeline.SegmentId}, because when a result
   * is sent from Historical to Broker server, on the deserialization side (Broker) it's impossible to unambiguously
   * convert a segment id string (as transmitted in the JSON format) back into a {@code SegmentId} object ({@link
   * org.apache.druid.timeline.SegmentId#tryParse} javadoc explains that ambiguities in details). It would be fine to
   * have the type of this field of Object, setting it to {@code SegmentId} on the Historical side and remaining as a
   * String on the Broker side, but it's even less type-safe than always storing the segment id as a String.
   */
  private final String segmentId;
  private final List<String> columns;
  private final Object events;

  @JsonCreator
  public ScanResultValue(
      @JsonProperty("segmentId") @Nullable String segmentId,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("events") Object events
  )
  {
    this.segmentId = segmentId;
    this.columns = columns;
    this.events = events;
  }

  @Nullable
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

  public long getFirstEventTimestamp(ScanQuery.ResultFormat resultFormat)
  {
    if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)) {
      Object timestampObj = ((Map<String, Object>) ((List<Object>) this.getEvents()).get(0)).get(ColumnHolder.TIME_COLUMN_NAME);
      if (timestampObj == null) {
        throw new ISE("Unable to compare timestamp for rows without a time column");
      }
      return DimensionHandlerUtils.convertObjectToLong(timestampObj);
    } else if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)) {
      int timeColumnIndex = this.getColumns().indexOf(ColumnHolder.TIME_COLUMN_NAME);
      if (timeColumnIndex == -1) {
        throw new ISE("Unable to compare timestamp for rows without a time column");
      }
      List<Object> firstEvent = (List<Object>) ((List<Object>) this.getEvents()).get(0);
      return DimensionHandlerUtils.convertObjectToLong(firstEvent.get(timeColumnIndex));
    }
    throw new UOE("Unable to get first event timestamp using result format of [%s]", resultFormat.toString());
  }

  public List<ScanResultValue> toSingleEventScanResultValues()
  {
    List<ScanResultValue> singleEventScanResultValues = new ArrayList<>();
    List<Object> events = (List<Object>) this.getEvents();
    for (Object event : events) {
      singleEventScanResultValues.add(new ScanResultValue(segmentId, columns, Collections.singletonList(event)));
    }
    return singleEventScanResultValues;
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
