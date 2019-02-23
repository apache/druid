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
  private static final long TIME_1 = 1234567890000L;
  private static final long TIME_2 = 9876543210000L;

  private static ScanResultValue compactedListSRV;
  private static ScanResultValue listSRV;

  @BeforeClass
  public static void setup()
  {
    String segmentId = "some_segment_id";
    List<String> columns = new ArrayList<>(Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, "name", "count"));
    List<Object> event = new ArrayList<>(Arrays.asList(
        TIME_1,
        "Feridun",
        4
    ));
    List<Object> event2 = new ArrayList<>(Arrays.asList(
        TIME_2,
        "Justin",
        6
    ));

    List<List<Object>> events = Arrays.asList(event, event2);
    compactedListSRV = new ScanResultValue(segmentId, columns, events);

    Map<String, Object> eventMap1 = new HashMap<>();
    eventMap1.put(ColumnHolder.TIME_COLUMN_NAME, TIME_1);
    eventMap1.put("name", "Feridun");
    eventMap1.put("count", 4);
    Map<String, Object> eventMap2 = new HashMap<>();
    eventMap2.put(ColumnHolder.TIME_COLUMN_NAME, TIME_2);
    eventMap2.put("name", "Justin");
    eventMap2.put("count", 6);
    List<Map<String, Object>> eventMaps = Arrays.asList(eventMap1, eventMap2);
    listSRV = new ScanResultValue(segmentId, columns, eventMaps);
  }

  @Test
  public void testSerdeScanResultValueCompactedList() throws IOException
  {

    String serialized = JSON_MAPPER.writeValueAsString(compactedListSRV);
    ScanResultValue deserialized = JSON_MAPPER.readValue(serialized, ScanResultValue.class);
    Assert.assertEquals(compactedListSRV, deserialized);
  }

  @Test
  public void testSerdeScanResultValueNonCompactedList() throws IOException
  {

    String serialized = JSON_MAPPER.writeValueAsString(listSRV);
    ScanResultValue deserialized = JSON_MAPPER.readValue(serialized, ScanResultValue.class);
    Assert.assertEquals(listSRV, deserialized);
  }

  @Test
  public void testGetFirstEventTimestampCompactedList()
  {
    long timestamp = compactedListSRV.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST);
    Assert.assertEquals(TIME_1, timestamp);
  }

  @Test
  public void testGetFirstEventTimestampNonCompactedList()
  {
    long timestamp = listSRV.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_LIST);
    Assert.assertEquals(TIME_1, timestamp);
  }

  @Test
  public void testToSingleEventScanResultValues()
  {
    List<ScanResultValue> compactedListScanResultValues = compactedListSRV.toSingleEventScanResultValues();
    for (ScanResultValue srv : compactedListScanResultValues) {
      List<Object> events = (List<Object>) srv.getEvents();
      Assert.assertEquals(1, events.size());
    }
    List<ScanResultValue> listScanResultValues = listSRV.toSingleEventScanResultValues();
    for (ScanResultValue srv : compactedListScanResultValues) {
      List<Object> events = (List<Object>) srv.getEvents();
      Assert.assertEquals(1, events.size());
    }
  }
}
