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

package org.apache.druid.query.movingaverage;

import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RowBucketIterableTest
{
  private static final DateTime JAN_1 = new DateTime(2017, 1, 1, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_2 = new DateTime(2017, 1, 2, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_3 = new DateTime(2017, 1, 3, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_4 = new DateTime(2017, 1, 4, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_5 = new DateTime(2017, 1, 5, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_6 = new DateTime(2017, 1, 6, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_9 = new DateTime(2017, 1, 9, 0, 0, 0, 0, ISOChronology.getInstanceUTC());

  private static final Map<String, Object> EVENT_M_10 = new HashMap<>();
  private static final Map<String, Object> EVENT_F_20 = new HashMap<>();
  private static final Map<String, Object> EVENT_U_30 = new HashMap<>();

  private static final Row JAN_1_M_10 = new MapBasedRow(new DateTime(2017, 1, 1, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_M_10);
  private static final Row JAN_1_F_20 = new MapBasedRow(new DateTime(2017, 1, 1, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_F_20);
  private static final Row JAN_1_U_30 = new MapBasedRow(new DateTime(2017, 1, 1, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_U_30);
  private static final Row JAN_2_M_10 = new MapBasedRow(new DateTime(2017, 1, 2, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_M_10);
  private static final Row JAN_3_M_10 = new MapBasedRow(new DateTime(2017, 1, 3, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_M_10);
  private static final Row JAN_3_F_20 = new MapBasedRow(new DateTime(2017, 1, 3, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_F_20);
  private static final Row JAN_4_M_10 = new MapBasedRow(new DateTime(2017, 1, 4, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_M_10);
  private static final Row JAN_4_F_20 = new MapBasedRow(new DateTime(2017, 1, 4, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_F_20);
  private static final Row JAN_4_U_30 = new MapBasedRow(new DateTime(2017, 1, 4, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_U_30);
  private static final Row JAN_5_M_10 = new MapBasedRow(new DateTime(2017, 1, 5, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_M_10);
  private static final Row JAN_6_M_10 = new MapBasedRow(new DateTime(2017, 1, 6, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_M_10);
  private static final Row JAN_7_F_20 = new MapBasedRow(new DateTime(2017, 1, 7, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_F_20);
  private static final Row JAN_8_U_30 = new MapBasedRow(new DateTime(2017, 1, 8, 0, 0, 0, 0, ISOChronology.getInstanceUTC()), EVENT_U_30);

  private static final Interval INTERVAL_JAN_1_1 = new Interval(JAN_1, JAN_2);
  private static final Interval INTERVAL_JAN_1_2 = new Interval(JAN_1, JAN_3);
  private static final Interval INTERVAL_JAN_1_4 = new Interval(JAN_1, JAN_5);
  private static final Interval INTERVAL_JAN_1_5 = new Interval(JAN_1, JAN_6);
  private static final Interval INTERVAL_JAN_6_8 = new Interval(JAN_6, JAN_9);
  private static final Period ONE_DAY = Period.days(1);

  private List<Row> rows = null;
  private List<Interval> intervals = new ArrayList<>();

  @BeforeClass
  public static void setupClass()
  {
    EVENT_M_10.put("gender", "m");
    EVENT_M_10.put("pageViews", 10L);
    EVENT_F_20.put("gender", "f");
    EVENT_F_20.put("pageViews", 20L);
    EVENT_U_30.put("gender", "u");
    EVENT_U_30.put("pageViews", 30L);
  }

  @Test
  public void testCompleteData()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_M_10);
    rows.add(JAN_4_M_10);

    List<Row> expectedDay1 = Collections.singletonList(JAN_1_M_10);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_M_10);
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testApplyLastDaySingleRow()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    List<Row> expectedDay1 = Arrays.asList(JAN_1_M_10, JAN_1_F_20);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_F_20);
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_1_F_20);
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_F_20);
    rows.add(JAN_4_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testApplyLastDayMultipleRows()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    List<Row> expectedDay1 = Arrays.asList(JAN_1_M_10, JAN_1_F_20);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_F_20);
    List<Row> expectedDay4 = Arrays.asList(JAN_4_M_10, JAN_4_F_20, JAN_4_U_30);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_1_F_20);
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_F_20);
    rows.add(JAN_4_M_10);
    rows.add(JAN_4_F_20);
    rows.add(JAN_4_U_30);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testSingleDaySingleRow()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_1);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);

    List<Row> expectedDay1 = Collections.singletonList(JAN_1_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());
    Assert.assertEquals(JAN_1, actual.getDateTime());
  }

  @Test
  public void testSingleDayMultipleRow()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_1);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_1_F_20);
    rows.add(JAN_1_U_30);

    List<Row> expectedDay1 = Arrays.asList(JAN_1_M_10, JAN_1_F_20, JAN_1_U_30);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());
  }

  @Test
  public void testMissingDaysAtBegining()
  {
    List<Row> expectedDay1 = Collections.emptyList();
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_2);

    rows = new ArrayList<>();
    rows.add(JAN_2_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());
  }

  @Test
  public void testMissingDaysAtBeginingFollowedByMultipleRow()
  {
    List<Row> expectedDay1 = Collections.emptyList();
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_M_10);
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    rows = new ArrayList<>();
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_M_10);
    rows.add(JAN_4_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testMissingDaysAtBeginingAndAtTheEnd()
  {
    List<Row> expectedDay1 = Collections.emptyList();
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_M_10);
    List<Row> expectedDay4 = Collections.emptyList();

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    rows = new ArrayList<>();
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testMultipleMissingDays()
  {
    List<Row> expectedDay1 = Collections.emptyList();
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.emptyList();
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    rows = new ArrayList<>();
    rows.add(JAN_2_M_10);
    rows.add(JAN_4_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testMultipleMissingDaysMultipleRowAtTheEnd()
  {
    List<Row> expectedDay1 = Collections.emptyList();
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.emptyList();
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);
    List<Row> expectedDay5 = Collections.singletonList(JAN_5_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_5);

    rows = new ArrayList<>();
    rows.add(JAN_2_M_10);
    rows.add(JAN_4_M_10);
    rows.add(JAN_5_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_5, actual.getDateTime());
    Assert.assertEquals(expectedDay5, actual.getRows());
  }

  @Test
  public void testMissingDaysInMiddleOneRow()
  {
    List<Row> expectedDay1 = Collections.singletonList(JAN_1_M_10);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.emptyList();
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_2_M_10);
    rows.add(JAN_4_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testMissingDaysInMiddleMultipleRow()
  {
    List<Row> expectedDay1 = Collections.singletonList(JAN_1_M_10);
    List<Row> expectedDay2 = Collections.emptyList();
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_M_10);
    List<Row> expectedDay4 = Collections.singletonList(JAN_4_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_3_M_10);
    rows.add(JAN_4_M_10);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(JAN_1, actual.getDateTime());
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_2, actual.getDateTime());
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testApplyLastDayNoRows()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    List<Row> expectedDay1 = Arrays.asList(JAN_1_M_10, JAN_1_F_20);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_F_20);
    List<Row> expectedDay4 = Collections.emptyList();

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_1_F_20);
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_F_20);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testApplyLastTwoDayNoRows()
  {
    List<Row> expectedDay1 = Arrays.asList(JAN_1_M_10, JAN_1_F_20);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.emptyList();
    List<Row> expectedDay4 = Collections.emptyList();

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_1_F_20);
    rows.add(JAN_2_M_10);

    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_3, actual.getDateTime());
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(JAN_4, actual.getDateTime());
    Assert.assertEquals(expectedDay4, actual.getRows());
  }

  @Test
  public void testApplyMultipleInterval()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);
    intervals.add(INTERVAL_JAN_6_8);

    List<Row> expectedDay1 = Arrays.asList(JAN_1_M_10, JAN_1_F_20);
    List<Row> expectedDay2 = Collections.singletonList(JAN_2_M_10);
    List<Row> expectedDay3 = Collections.singletonList(JAN_3_F_20);
    List<Row> expectedDay4 = Arrays.asList(JAN_4_M_10, JAN_4_F_20, JAN_4_U_30);
    List<Row> expectedDay6 = Collections.singletonList(JAN_6_M_10);
    List<Row> expectedDay7 = Collections.singletonList(JAN_7_F_20);
    List<Row> expectedDay8 = Collections.singletonList(JAN_8_U_30);

    rows = new ArrayList<>();
    rows.add(JAN_1_M_10);
    rows.add(JAN_1_F_20);
    rows.add(JAN_2_M_10);
    rows.add(JAN_3_F_20);
    rows.add(JAN_4_M_10);
    rows.add(JAN_4_F_20);
    rows.add(JAN_4_U_30);
    rows.add(JAN_6_M_10);
    rows.add(JAN_7_F_20);
    rows.add(JAN_8_U_30);

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    RowBucket actual = iter.next();
    Assert.assertEquals(expectedDay1, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay2, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay3, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay4, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay6, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay7, actual.getRows());

    actual = iter.next();
    Assert.assertEquals(expectedDay8, actual.getRows());
  }

  @Test
  public void testNodata()
  {
    intervals = new ArrayList<>();
    intervals.add(INTERVAL_JAN_1_4);
    intervals.add(INTERVAL_JAN_6_8);

    rows = new ArrayList<>();

    Sequence<Row> seq = Sequences.simple(rows);
    RowBucketIterable rbi = new RowBucketIterable(seq, intervals, ONE_DAY);
    Iterator<RowBucket> iter = rbi.iterator();

    Assert.assertTrue(iter.hasNext());
    RowBucket actual = iter.next();
    Assert.assertEquals(Collections.emptyList(), actual.getRows());
  }
}
