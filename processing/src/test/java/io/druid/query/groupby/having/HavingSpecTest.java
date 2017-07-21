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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class HavingSpecTest
{
  private static final Row ROW = new MapBasedInputRow(0, new ArrayList<String>(), ImmutableMap.of("metric", (Object)Float.valueOf(10)));

  @Test
  public void testHavingClauseSerde() throws Exception
  {
    List<HavingSpec> havings = Arrays.<HavingSpec>asList(
      new GreaterThanHavingSpec("agg", Double.valueOf(1.3)),
      new OrHavingSpec(
        Arrays.<HavingSpec>asList(
          new LessThanHavingSpec("lessAgg", Long.valueOf(1L)),
          new NotHavingSpec(new EqualToHavingSpec("equalAgg", Double.valueOf(2)))
        )
      )
    );

    HavingSpec andHavingSpec = new AndHavingSpec(havings);

    Map<String, Object> notMap = ImmutableMap.<String, Object>of(
        "type", "not",
        "havingSpec", ImmutableMap.of("type", "equalTo", "aggregation", "equalAgg", "value", 2.0)
    );

    Map<String, Object> lessMap = ImmutableMap.<String, Object>of(
        "type", "lessThan",
        "aggregation", "lessAgg",
        "value", 1
    );

    Map<String, Object> greaterMap = ImmutableMap.<String, Object>of(
        "type", "greaterThan",
        "aggregation", "agg",
        "value", 1.3
    );

    Map<String, Object> orMap = ImmutableMap.<String, Object>of(
        "type", "or",
        "havingSpecs", ImmutableList.of(lessMap, notMap)
    );

    Map<String, Object> payloadMap = ImmutableMap.<String, Object>of(
        "type", "and",
        "havingSpecs", ImmutableList.of(greaterMap, orMap)
    );

    ObjectMapper mapper = new DefaultObjectMapper();
    assertEquals(andHavingSpec,  mapper.convertValue(payloadMap, AndHavingSpec.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeTypo()
  {
    Map<String, Object> greaterMap = ImmutableMap.<String, Object>of(
        "type", "nonExistingType",
        "aggregation", "agg",
        "value", 1.3
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    HavingSpec spec = mapper.convertValue (greaterMap, HavingSpec.class);

  }

  @Test
  public void testGreaterThanHavingSpec()
  {
    GreaterThanHavingSpec spec = new GreaterThanHavingSpec("metric", Long.valueOf(Long.MAX_VALUE - 10));
    assertFalse(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 10))));
    assertFalse(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 15))));
    assertTrue(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 5))));
    assertTrue(spec.eval(getTestRow(String.valueOf(Long.MAX_VALUE - 5))));
    assertFalse(spec.eval(getTestRow(100.05f)));

    spec = new GreaterThanHavingSpec("metric", 100.56f);
    assertFalse(spec.eval(getTestRow(100.56f)));
    assertFalse(spec.eval(getTestRow(90.53f)));
    assertFalse(spec.eval(getTestRow("90.53f")));
    assertTrue(spec.eval(getTestRow(101.34f)));
    assertTrue(spec.eval(getTestRow(Long.MAX_VALUE)));
  }

  @Test
  public void testLessThanHavingSpec()
  {
    LessThanHavingSpec spec = new LessThanHavingSpec("metric", Long.valueOf(Long.MAX_VALUE - 10));
    assertFalse(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 10))));
    assertTrue(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 15))));
    assertTrue(spec.eval(getTestRow(String.valueOf(Long.MAX_VALUE - 15))));
    assertFalse(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 5))));
    assertTrue(spec.eval(getTestRow(100.05f)));

    spec = new LessThanHavingSpec("metric", 100.56f);
    assertFalse(spec.eval(getTestRow(100.56f)));
    assertTrue(spec.eval(getTestRow(90.53f)));
    assertFalse(spec.eval(getTestRow(101.34f)));
    assertFalse(spec.eval(getTestRow("101.34f")));
    assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));
  }

  private Row getTestRow(Object metricValue)
  {
    return new MapBasedInputRow(0, new ArrayList<String>(), ImmutableMap.of("metric", metricValue));
  }

  @Test
  public void testEqualHavingSpec()
  {
    EqualToHavingSpec spec = new EqualToHavingSpec("metric", Long.valueOf(Long.MAX_VALUE - 10));
    assertTrue(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 10))));
    assertFalse(spec.eval(getTestRow(Long.valueOf(Long.MAX_VALUE - 5))));
    assertFalse(spec.eval(getTestRow(100.05f)));

    spec = new EqualToHavingSpec("metric", 100.56f);
    assertTrue(spec.eval(getTestRow(100.56f)));
    assertFalse(spec.eval(getTestRow(90.53f)));
    assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));
  }

  private static class CountingHavingSpec extends BaseHavingSpec
  {

    private final AtomicInteger counter;
    private final boolean value;
    private CountingHavingSpec(AtomicInteger counter, boolean value)
    {
      this.counter = counter;
      this.value = value;
    }

    @Override
    public boolean eval(Row row)
    {
      counter.incrementAndGet();
      return value;
    }
  }

  @Test
  public void testAndHavingSpecShouldSupportShortcutEvaluation()
  {
    AtomicInteger counter = new AtomicInteger(0);
    AndHavingSpec spec = new AndHavingSpec(ImmutableList.of(
      (HavingSpec)new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, false)
    ));

    spec.eval(ROW);

    assertEquals(2, counter.get());
  }

  @Test
  public void testAndHavingSpec()
  {
    AtomicInteger counter = new AtomicInteger(0);
    AndHavingSpec spec = new AndHavingSpec(ImmutableList.of(
      (HavingSpec)new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true)
    ));

    spec.eval(ROW);

    assertEquals(4, counter.get());

    counter.set(0);
    spec = new AndHavingSpec(ImmutableList.of(
      (HavingSpec)new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true)
    ));

    spec.eval(ROW);

    assertEquals(1, counter.get());
  }

  @Test
  public void testOrHavingSpecSupportsShortcutEvaluation()
  {
    AtomicInteger counter = new AtomicInteger(0);
    OrHavingSpec spec = new OrHavingSpec(ImmutableList.of(
      (HavingSpec)new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, true),
      new CountingHavingSpec(counter, false)
    ));

    spec.eval(ROW);

    assertEquals(1, counter.get());
  }

  @Test
  public void testOrHavingSpec()
  {
    AtomicInteger counter = new AtomicInteger(0);
    OrHavingSpec spec = new OrHavingSpec(ImmutableList.of(
      (HavingSpec)new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, false)
    ));

    spec.eval(ROW);

    assertEquals(4, counter.get());

    counter.set(0);
    spec = new OrHavingSpec(ImmutableList.of(
      (HavingSpec)new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, false),
      new CountingHavingSpec(counter, true)
    ));

    spec.eval(ROW);

    assertEquals(4, counter.get());
  }

  @Test
  public void testNotHavingSepc()
  {
    NotHavingSpec spec = new NotHavingSpec(HavingSpec.NEVER);
    assertTrue(spec.eval(ROW));

    spec = new NotHavingSpec(HavingSpec.ALWAYS);
    assertFalse(spec.eval(ROW));

  }
}
