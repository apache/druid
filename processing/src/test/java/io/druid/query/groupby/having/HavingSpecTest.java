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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
  public void testHavingClauseSerde() throws Exception {
    List<HavingSpec> havings = Arrays.asList(
      new GreaterThanHavingSpec("agg", Double.valueOf(1.3)),
      new OrHavingSpec(
        Arrays.asList(
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

  @Test
  public void testGreaterThanHavingSpec() {
    GreaterThanHavingSpec spec = new GreaterThanHavingSpec("metric", 10.003);
    assertFalse(spec.eval(ROW));

    spec = new GreaterThanHavingSpec("metric", 10);
    assertFalse(spec.eval(ROW));

    spec = new GreaterThanHavingSpec("metric", 9);
    assertTrue(spec.eval(ROW));
  }

  private MapBasedInputRow makeRow(long ts, String dim, int value)
  {
    List<String> dimensions = Lists.newArrayList(dim);
    Map<String, Object> metrics = ImmutableMap.of("metric", (Object) Float.valueOf(value));

    return new MapBasedInputRow(ts, dimensions, metrics);
  }

  @Test
  public void testLessThanHavingSpec() {
    LessThanHavingSpec spec = new LessThanHavingSpec("metric", 10);
    assertFalse(spec.eval(ROW));

    spec = new LessThanHavingSpec("metric", 11);
    assertTrue(spec.eval(ROW));

    spec = new LessThanHavingSpec("metric", 9);
    assertFalse(spec.eval(ROW));
  }

  @Test
  public void testEqualHavingSpec() {
    EqualToHavingSpec spec = new EqualToHavingSpec("metric", 10);
    assertTrue(spec.eval(ROW));

    spec = new EqualToHavingSpec("metric", 9);
    assertFalse(spec.eval(ROW));

    spec = new EqualToHavingSpec("metric", 11);
    assertFalse(spec.eval(ROW));
  }

  private static class CountingHavingSpec implements HavingSpec {

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

    @Override
    public byte[] getCacheKey()
    {
      return new byte[0];
    }
  }

  @Test
  public void testAndHavingSpecShouldSupportShortcutEvaluation () {
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
  public void testAndHavingSpec () {
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
  public void testOrHavingSpecSupportsShortcutEvaluation() {
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
  public void testOrHavingSpec () {
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
  public void testNotHavingSepc() {
    NotHavingSpec spec = new NotHavingSpec(HavingSpec.NEVER);
    assertTrue(spec.eval(ROW));

    spec = new NotHavingSpec(HavingSpec.ALWAYS);
    assertFalse(spec.eval(ROW));

  }
}
