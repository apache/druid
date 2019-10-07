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
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.movingaverage.averagers.AveragerFactory;
import org.apache.druid.query.movingaverage.averagers.ConstantAveragerFactory;
import org.apache.druid.query.movingaverage.averagers.LongMeanAveragerFactory;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MovingAverageIterableTest
{
  private static final DateTime JAN_1 = new DateTime(2017, 1, 1, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_2 = new DateTime(2017, 1, 2, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_3 = new DateTime(2017, 1, 3, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_4 = new DateTime(2017, 1, 4, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_5 = new DateTime(2017, 1, 5, 0, 0, 0, 0, ISOChronology.getInstanceUTC());
  private static final DateTime JAN_6 = new DateTime(2017, 1, 6, 0, 0, 0, 0, ISOChronology.getInstanceUTC());

  private static final String GENDER = "gender";
  private static final String AGE = "age";
  private static final String COUNTRY = "country";

  private static final Map<String, Object> DIMS1 = new HashMap<>();
  private static final Map<String, Object> DIMS2 = new HashMap<>();
  private static final Map<String, Object> DIMS3 = new HashMap<>();

  static {
    DIMS1.put(GENDER, "m");
    DIMS1.put(AGE, "10");
    DIMS1.put(COUNTRY, "US");

    DIMS2.put(GENDER, "f");
    DIMS2.put(AGE, "8");
    DIMS2.put(COUNTRY, "US");

    DIMS3.put(GENDER, "u");
    DIMS3.put(AGE, "5");
    DIMS3.put(COUNTRY, "UK");
  }

  @Test
  public void testNext()
  {

    List<DimensionSpec> dims = Arrays.asList(
        new DefaultDimensionSpec(GENDER, GENDER),
        new DefaultDimensionSpec(AGE, AGE),
        new DefaultDimensionSpec(COUNTRY, COUNTRY)
    );

    Sequence<RowBucket> dayBuckets = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Arrays.asList(
            new MapBasedRow(JAN_1, DIMS1),
            new MapBasedRow(JAN_1, DIMS2)
        )),
        new RowBucket(JAN_2, Collections.singletonList(
            new MapBasedRow(JAN_2, DIMS1)
        )),
        new RowBucket(JAN_3, Collections.emptyList()),
        new RowBucket(JAN_4, Arrays.asList(
            new MapBasedRow(JAN_4, DIMS2),
            new MapBasedRow(JAN_4, DIMS3)
        ))
    ));

    Iterable<Row> iterable = new MovingAverageIterable(
        dayBuckets,
        dims,
        Collections.singletonList(new ConstantAveragerFactory("noop", 1, 1.1f)),
        Collections.emptyList(),
        Collections.emptyList()
    );

    Iterator<Row> iter = iterable.iterator();

    Assert.assertTrue(iter.hasNext());
    Row r = iter.next();
    Assert.assertEquals(JAN_1, r.getTimestamp());
    Assert.assertEquals("m", r.getRaw(GENDER));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_1, r.getTimestamp());
    Assert.assertEquals("f", r.getRaw(GENDER));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_2, r.getTimestamp());
    Assert.assertEquals("m", r.getRaw(GENDER));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_2, r.getTimestamp());
    Assert.assertEquals("f", r.getRaw(GENDER));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Row r2 = r;
    Assert.assertEquals(JAN_3, r.getTimestamp());
    Assert.assertEquals("US", r.getRaw(COUNTRY));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_3, r.getTimestamp());
    Assert.assertEquals("US", r.getRaw(COUNTRY));
    Assert.assertThat(r.getRaw(AGE), CoreMatchers.not(CoreMatchers.equalTo(r2.getRaw(AGE))));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_4, r.getTimestamp());
    Assert.assertEquals("f", r.getRaw(GENDER));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_4, r.getTimestamp());
    Assert.assertEquals("u", r.getRaw(GENDER));

    Assert.assertTrue(iter.hasNext());
    r = iter.next();
    Assert.assertEquals(JAN_4, r.getTimestamp());
    Assert.assertEquals("m", r.getRaw(GENDER));

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testAveraging()
  {

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();
    Map<String, Object> event3 = new HashMap<>();
    Map<String, Object> event4 = new HashMap<>();

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    Row row1 = new MapBasedRow(JAN_1, event1);

    event2.put("gender", "m");
    event2.put("pageViews", 20L);
    Row row2 = new MapBasedRow(JAN_2, event2);

    event3.put("gender", "m");
    event3.put("pageViews", 30L);
    Row row3 = new MapBasedRow(JAN_3, event3);

    event4.put("gender", "f");
    event4.put("pageViews", 40L);
    Row row4 = new MapBasedRow(JAN_3, event4);

    float retval = 14.5f;

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Collections.singletonList(row1)),
        new RowBucket(JAN_2, Collections.singletonList(row2)),
        new RowBucket(JAN_3, Arrays.asList(row3, row4))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Arrays.asList(
            new ConstantAveragerFactory("costPageViews", 7, retval),
            new LongMeanAveragerFactory("movingAvgPageViews", 7, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row caResult = iter.next();

    Assert.assertEquals(JAN_1, caResult.getTimestamp());
    Assert.assertEquals("m", (caResult.getDimension("gender")).get(0));
    Assert.assertEquals(retval, caResult.getMetric("costPageViews").floatValue(), 0.0f);
    Assert.assertEquals(1.4285715f, caResult.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    caResult = iter.next();
    Assert.assertEquals("m", (caResult.getDimension("gender")).get(0));
    Assert.assertEquals(4.285714f, caResult.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    caResult = iter.next();
    Assert.assertEquals("m", (caResult.getDimension("gender")).get(0));
    Assert.assertEquals(8.571428f, caResult.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    caResult = iter.next();
    Assert.assertEquals("f", (caResult.getDimension("gender")).get(0));
    Assert.assertEquals(5.714285850f, caResult.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertFalse(iter.hasNext());

  }


  @Test
  public void testCompleteData()
  {

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();
    Map<String, Object> event3 = new HashMap<>();

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    event2.put("gender", "f");
    event2.put("pageViews", 20L);
    event3.put("gender", "u");
    event3.put("pageViews", 30L);

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    Row jan1Row1 = new MapBasedRow(JAN_1, event1);
    Row jan1Row2 = new MapBasedRow(JAN_1, event2);
    Row jan1Row3 = new MapBasedRow(JAN_1, event3);

    Row jan2Row1 = new MapBasedRow(JAN_2, event1);
    Row jan2Row2 = new MapBasedRow(JAN_2, event2);
    Row jan2Row3 = new MapBasedRow(JAN_2, event3);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Arrays.asList(jan1Row1, jan1Row2, jan1Row3)),
        new RowBucket(JAN_2, Arrays.asList(jan2Row1, jan2Row2, jan2Row3))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 2, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertFalse(iter.hasNext());

  }

  // no injection if the data missing at the begining
  @Test
  public void testMissingDataAtBeginning()
  {

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();
    Map<String, Object> event3 = new HashMap<>();

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    event2.put("gender", "f");
    event2.put("pageViews", 20L);
    event3.put("gender", "u");
    event3.put("pageViews", 30L);

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    Row jan1Row1 = new MapBasedRow(JAN_1, event1);

    Row jan2Row1 = new MapBasedRow(JAN_2, event1);
    Row jan2Row2 = new MapBasedRow(JAN_2, event2);
    Row jan2Row3 = new MapBasedRow(JAN_2, event3);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Collections.singletonList(jan1Row1)),
        new RowBucket(JAN_2, Arrays.asList(jan2Row1, jan2Row2, jan2Row3))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 2, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertFalse(iter.hasNext());
  }

  // test injection when the data is missing at the end
  @Test
  public void testMissingDataAtTheEnd()
  {

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();
    Map<String, Object> event3 = new HashMap<>();

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    event2.put("gender", "f");
    event2.put("pageViews", 20L);
    event3.put("gender", "u");
    event3.put("pageViews", 30L);

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    Row jan1Row1 = new MapBasedRow(JAN_1, event1);
    Row jan1Row2 = new MapBasedRow(JAN_1, event2);
    Row jan1Row3 = new MapBasedRow(JAN_1, event3);
    Row jan2Row1 = new MapBasedRow(JAN_2, event1);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Arrays.asList(jan1Row1, jan1Row2, jan1Row3)),
        new RowBucket(JAN_2, Collections.singletonList(jan2Row1))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 2, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertFalse(iter.hasNext());
  }

  // test injection when the data is missing in the middle
  @Test
  public void testMissingDataAtMiddle()
  {

    Map<String, Object> eventM = new HashMap<>();
    Map<String, Object> eventF = new HashMap<>();
    Map<String, Object> eventU = new HashMap<>();

    eventM.put("gender", "m");
    eventM.put("pageViews", 10L);
    eventF.put("gender", "f");
    eventF.put("pageViews", 20L);
    eventU.put("gender", "u");
    eventU.put("pageViews", 30L);

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    Row jan1Row1M = new MapBasedRow(JAN_1, eventM);
    Row jan1Row2F = new MapBasedRow(JAN_1, eventF);
    Row jan1Row3U = new MapBasedRow(JAN_1, eventU);
    Row jan2Row1M = new MapBasedRow(JAN_2, eventM);
    Row jan3Row1M = new MapBasedRow(JAN_3, eventM);
    Row jan3Row2F = new MapBasedRow(JAN_3, eventF);
    Row jan3Row3U = new MapBasedRow(JAN_3, eventU);
    Row jan4Row1M = new MapBasedRow(JAN_4, eventM);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Arrays.asList(jan1Row1M, jan1Row2F, jan1Row3U)),
        new RowBucket(JAN_2, Collections.singletonList(jan2Row1M)),
        new RowBucket(JAN_3, Arrays.asList(jan3Row1M, jan3Row2F, jan3Row3U)),
        new RowBucket(JAN_4, Collections.singletonList(jan4Row1M))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 3, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    // Jan 1
    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_1, (result.getTimestamp()));

    // Jan 2
    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_2, (result.getTimestamp()));

    // Jan 3
    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_3, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_3, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_3, (result.getTimestamp()));

    // Jan 4
    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_4, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("u", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_4, (result.getTimestamp()));

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("f", (result.getDimension("gender")).get(0));
    Assert.assertEquals(JAN_4, (result.getTimestamp()));

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testMissingDaysAtBegining()
  {

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    Row row1 = new MapBasedRow(JAN_3, event1);

    event2.put("gender", "m");
    event2.put("pageViews", 20L);
    Row row2 = new MapBasedRow(JAN_4, event2);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Collections.emptyList()),
        new RowBucket(JAN_2, Collections.emptyList()),
        new RowBucket(JAN_3, Collections.singletonList(row1)),
        new RowBucket(JAN_4, Collections.singletonList(row2))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 4, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(7.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testMissingDaysInMiddle()
  {
    System.setProperty("druid.generic.useDefaultValueForNull", "true");

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    Row row1 = new MapBasedRow(JAN_1, event1);

    event2.put("gender", "m");
    event2.put("pageViews", 20L);
    Row row2 = new MapBasedRow(JAN_4, event2);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Collections.singletonList(row1)),
        new RowBucket(JAN_2, Collections.emptyList()),
        new RowBucket(JAN_3, Collections.emptyList()),
        new RowBucket(JAN_4, Collections.singletonList(row2))
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 4, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(7.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testWithFilteredAggregation()
  {

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    Row row1 = new MapBasedRow(JAN_1, event1);

    event2.put("gender", "m");
    event2.put("pageViews", 20L);
    Row row2 = new MapBasedRow(JAN_4, event2);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Collections.singletonList(row1)),
        new RowBucket(JAN_2, Collections.emptyList()),
        new RowBucket(JAN_3, Collections.emptyList()),
        new RowBucket(JAN_4, Collections.singletonList(row2))
    ));

    AveragerFactory averagerfactory = new LongMeanAveragerFactory("movingAvgPageViews", 4, 1, "pageViews");
    AggregatorFactory aggregatorFactory = new LongSumAggregatorFactory("pageViews", "pageViews");
    DimFilter filter = new SelectorDimFilter("gender", "m", null);
    FilteredAggregatorFactory filteredAggregatorFactory = new FilteredAggregatorFactory(aggregatorFactory, filter);

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(averagerfactory),
        Collections.emptyList(),
        Collections.singletonList(filteredAggregatorFactory)
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(7.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testMissingDaysAtEnd()
  {
    System.setProperty("druid.generic.useDefaultValueForNull", "true");

    Map<String, Object> event1 = new HashMap<>();
    Map<String, Object> event2 = new HashMap<>();

    List<DimensionSpec> ds = new ArrayList<>();
    ds.add(new DefaultDimensionSpec("gender", "gender"));

    event1.put("gender", "m");
    event1.put("pageViews", 10L);
    Row row1 = new MapBasedRow(JAN_1, event1);

    event2.put("gender", "m");
    event2.put("pageViews", 20L);
    Row row2 = new MapBasedRow(JAN_2, event2);

    Sequence<RowBucket> seq = Sequences.simple(Arrays.asList(
        new RowBucket(JAN_1, Collections.singletonList(row1)),
        new RowBucket(JAN_2, Collections.singletonList(row2)),
        new RowBucket(JAN_3, Collections.emptyList()),
        new RowBucket(JAN_4, Collections.emptyList()),
        new RowBucket(JAN_5, Collections.emptyList()),
        new RowBucket(JAN_6, Collections.emptyList())
    ));

    Iterator<Row> iter = new MovingAverageIterable(
        seq,
        ds,
        Collections.singletonList(
            new LongMeanAveragerFactory("movingAvgPageViews", 4, 1, "pageViews")
        ),
        Collections.emptyList(),
        Collections.singletonList(new LongSumAggregatorFactory("pageViews", "pageViews"))
    ).iterator();

    Assert.assertTrue(iter.hasNext());
    Row result = iter.next();

    Assert.assertEquals(JAN_1, result.getTimestamp());
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(2.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals(JAN_2, result.getTimestamp());
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(7.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals(JAN_3, result.getTimestamp());
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(7.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals(JAN_4, result.getTimestamp());
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(7.5f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals(JAN_5, result.getTimestamp());
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(5.0f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertTrue(iter.hasNext());
    result = iter.next();
    Assert.assertEquals(JAN_6, result.getTimestamp());
    Assert.assertEquals("m", (result.getDimension("gender")).get(0));
    Assert.assertEquals(0.0f, result.getMetric("movingAvgPageViews").floatValue(), 0.0f);

    Assert.assertFalse(iter.hasNext());
  }
}
