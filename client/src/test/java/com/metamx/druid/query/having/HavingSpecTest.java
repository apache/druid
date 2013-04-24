package com.metamx.druid.query.having;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.query.having.*;
import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.util.*;
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

    String havingClausePayload = "{\"type\":\"and\",\"havingSpecs\":[{\"type\":\"greaterThan\",\"aggregation\":\"agg\",\"value\":1.3},{\"type\":\"or\",\"havingSpecs\":[{\"type\":\"lessThan\",\"aggregation\":\"lessAgg\",\"value\":1},{\"type\":\"not\",\"havingSpec\":{\"type\":\"equalTo\", \"aggregation\":\"equalAgg\", \"value\":2.0}}]}]}";

    ObjectMapper mapper = new DefaultObjectMapper();
    assertEquals(andHavingSpec,  mapper.readValue(havingClausePayload, AndHavingSpec.class));
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
