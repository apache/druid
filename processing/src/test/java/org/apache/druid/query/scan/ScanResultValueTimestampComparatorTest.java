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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Druids;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ScanResultValueTimestampComparatorTest
{
  private static QuerySegmentSpec intervalSpec;

  @BeforeClass
  public static void setup()
  {
    intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(
            new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1))
        )
    );
  }

  @Test
  public void testComparisonDescendingList()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                            .dataSource("some src")
                            .intervals(intervalSpec)
                            .build();

    ScanResultValueTimestampComparator comparator = new ScanResultValueTimestampComparator(query);

    ArrayList<HashMap<String, Object>> events1 = new ArrayList<>();
    HashMap<String, Object> event1 = new HashMap<>();
    event1.put(ColumnHolder.TIME_COLUMN_NAME, new Long(42));
    events1.add(event1);

    ScanResultValue s1 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events1
    );

    ArrayList<HashMap<String, Object>> events2 = new ArrayList<>();
    HashMap<String, Object> event2 = new HashMap<>();
    event2.put(ColumnHolder.TIME_COLUMN_NAME, new Long(43));
    events2.add(event2);

    ScanResultValue s2 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events2
    );

    Assert.assertEquals(-1, comparator.compare(s1, s2));
  }

  @Test
  public void testComparisonAscendingList()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .order(ScanQuery.Order.ASCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                            .dataSource("some src")
                            .intervals(intervalSpec)
                            .build();

    ScanResultValueTimestampComparator comparator = new ScanResultValueTimestampComparator(query);

    ArrayList<HashMap<String, Object>> events1 = new ArrayList<>();
    HashMap<String, Object> event1 = new HashMap<>();
    event1.put(ColumnHolder.TIME_COLUMN_NAME, new Long(42));
    events1.add(event1);

    ScanResultValue s1 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events1
    );

    ArrayList<HashMap<String, Object>> events2 = new ArrayList<>();
    HashMap<String, Object> event2 = new HashMap<>();
    event2.put(ColumnHolder.TIME_COLUMN_NAME, new Long(43));
    events2.add(event2);

    ScanResultValue s2 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events2
    );

    Assert.assertEquals(1, comparator.compare(s1, s2));
  }

  @Test
  public void testComparisonDescendingCompactedList()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .dataSource("some src")
                            .intervals(intervalSpec)
                            .build();

    ScanResultValueTimestampComparator comparator = new ScanResultValueTimestampComparator(query);

    List<List<Object>> events1 = new ArrayList<>();
    List<Object> event1 = Collections.singletonList(new Long(42));
    events1.add(event1);

    ScanResultValue s1 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events1
    );

    List<List<Object>> events2 = new ArrayList<>();
    List<Object> event2 = Collections.singletonList(new Long(43));
    events2.add(event2);

    ScanResultValue s2 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events2
    );

    Assert.assertEquals(-1, comparator.compare(s1, s2));
  }

  @Test
  public void testAscendingCompactedList()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .order(ScanQuery.Order.ASCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .dataSource("some src")
                            .intervals(intervalSpec)
                            .build();

    ScanResultValueTimestampComparator comparator = new ScanResultValueTimestampComparator(query);

    List<List<Object>> events1 = new ArrayList<>();
    List<Object> event1 = Collections.singletonList(new Long(42));
    events1.add(event1);

    ScanResultValue s1 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events1
    );

    List<List<Object>> events2 = new ArrayList<>();
    List<Object> event2 = Collections.singletonList(new Long(43));
    events2.add(event2);

    ScanResultValue s2 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events2
    );

    Assert.assertEquals(1, comparator.compare(s1, s2));
  }
}
