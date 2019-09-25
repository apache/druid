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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanResultValueTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final long TIME_1_LONG = 1234567890000L;
  private static final long TIME_2_LONG = 9876543210000L;
  private static final int TIME_3_INT = Integer.MAX_VALUE;

  private static ScanResultValue compactedListSRVLongTimestamp;
  private static ScanResultValue listSRVLongTimestamp;
  private static ScanResultValue compactedListSRVIntegerTimestamp;
  private static ScanResultValue listSRVIntegerTimestamp;

  @BeforeClass
  public static void setup()
  {
    String segmentId = "some_segment_id";
    List<String> columns = new ArrayList<>(Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, "name", "count"));
    List<Object> event = new ArrayList<>(Arrays.asList(
        TIME_1_LONG,
        "Feridun",
        4
    ));
    List<Object> event2 = new ArrayList<>(Arrays.asList(
        TIME_2_LONG,
        "Justin",
        6
    ));

    List<List<Object>> events = Arrays.asList(event, event2);
    compactedListSRVLongTimestamp = new ScanResultValue(segmentId, columns, events);

    List<Object> eventInt = new ArrayList<>(Arrays.asList(
        TIME_3_INT,
        "Feridun",
        4
    ));

    List<List<Object>> eventsInt = Arrays.asList(eventInt, event2);
    compactedListSRVIntegerTimestamp = new ScanResultValue(segmentId, columns, eventsInt);

    Map<String, Object> eventMap1 = new HashMap<>();
    eventMap1.put(ColumnHolder.TIME_COLUMN_NAME, TIME_1_LONG);
    eventMap1.put("name", "Feridun");
    eventMap1.put("count", 4);
    Map<String, Object> eventMap2 = new HashMap<>();
    eventMap2.put(ColumnHolder.TIME_COLUMN_NAME, TIME_2_LONG);
    eventMap2.put("name", "Justin");
    eventMap2.put("count", 6);
    List<Map<String, Object>> eventMaps = Arrays.asList(eventMap1, eventMap2);
    listSRVLongTimestamp = new ScanResultValue(segmentId, columns, eventMaps);

    Map<String, Object> eventMap3 = new HashMap<>();
    eventMap3.put(ColumnHolder.TIME_COLUMN_NAME, TIME_3_INT);
    eventMap3.put("name", "Justin");
    eventMap3.put("count", 6);
    List<Map<String, Object>> eventMapsInt = Arrays.asList(eventMap3, eventMap2);
    listSRVIntegerTimestamp = new ScanResultValue(segmentId, columns, eventMapsInt);
  }

  @Test
  public void testSerdeScanResultValueCompactedList() throws IOException
  {

    String serialized = JSON_MAPPER.writeValueAsString(compactedListSRVLongTimestamp);
    ScanResultValue deserialized = JSON_MAPPER.readValue(serialized, ScanResultValue.class);
    Assert.assertEquals(compactedListSRVLongTimestamp, deserialized);
  }

  @Test
  public void testSerdeScanResultValueNonCompactedList() throws IOException
  {

    String serialized = JSON_MAPPER.writeValueAsString(listSRVLongTimestamp);
    ScanResultValue deserialized = JSON_MAPPER.readValue(serialized, ScanResultValue.class);
    Assert.assertEquals(listSRVLongTimestamp, deserialized);
  }

  @Test
  public void testGetFirstEventTimestampCompactedList()
  {
    long timestamp = compactedListSRVLongTimestamp.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST);
    Assert.assertEquals(TIME_1_LONG, timestamp);
    long timestampInt = compactedListSRVIntegerTimestamp.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST);
    Assert.assertEquals(TIME_3_INT, timestampInt);
  }

  @Test
  public void testGetFirstEventTimestampNonCompactedList()
  {
    long timestamp = listSRVLongTimestamp.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_LIST);
    Assert.assertEquals(TIME_1_LONG, timestamp);
    long timestampInt = listSRVIntegerTimestamp.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_LIST);
    Assert.assertEquals(TIME_3_INT, timestampInt);
  }

  @Test
  public void testToSingleEventScanResultValues()
  {
    List<ScanResultValue> compactedListScanResultValues = compactedListSRVLongTimestamp.toSingleEventScanResultValues();
    for (ScanResultValue srv : compactedListScanResultValues) {
      List<Object> events = (List<Object>) srv.getEvents();
      Assert.assertEquals(1, events.size());
    }
    List<ScanResultValue> listScanResultValues = listSRVLongTimestamp.toSingleEventScanResultValues();
    for (ScanResultValue srv : listScanResultValues) {
      List<Object> events = (List<Object>) srv.getEvents();
      Assert.assertEquals(1, events.size());
    }
  }
}
