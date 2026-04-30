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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.client.DruidServer;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PartialLoadRuleTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final DateTime NOW = DateTimes.of("2024-06-01T00:00:00");
  private static final Interval IN_WINDOW = Intervals.of("2024-05-15/2024-05-20");
  private static final Interval OUT_OF_WINDOW = Intervals.of("2020-01-01/2020-02-01");

  @Test
  void testAppliesToOutsideWindowReturnsFalseRegardlessOfMatcher()
  {
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        null
    );
    DataSegment segment = segmentWithProjections(OUT_OF_WINDOW, List.of("a", "b"));
    Assertions.assertFalse(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToWindowMatchAndMatcherProducesResultReturnsTrue()
  {
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        null
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertTrue(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToMatcherDoesNotApplyFallThroughReturnsFalse()
  {
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("nonexistent"),
        CannotMatchBehavior.FALL_THROUGH
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertFalse(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToMatcherDoesNotApplyFullLoadIsDefault()
  {
    // FULL_LOAD is the default. Segment has projections but matcher resolves to nothing,
    // so the rule still applies and the segment gets full-loaded on this tier.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("nonexistent"),
        null
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertTrue(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToProjectionAgnosticSegmentFallThrough()
  {
    // Pre-Druid-32 segment: projections == null. With FALL_THROUGH, rule does not apply.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        CannotMatchBehavior.FALL_THROUGH
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, null);
    Assertions.assertFalse(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToProjectionAgnosticSegmentFullLoad()
  {
    // Default behavior: pre-Druid-32 segments fall into full-load on this tier.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        null
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, null);
    Assertions.assertTrue(rule.appliesTo(segment, NOW));
  }

  @Test
  void testIntervalOverloadIgnoresMatcher()
  {
    // The Interval-based appliesTo overload has no segment context, so it can only check the time window. Matcher
    // logic does not run.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("nonexistent"),
        null
    );
    Assertions.assertTrue(rule.appliesTo(IN_WINDOW, NOW));
    Assertions.assertFalse(rule.appliesTo(OUT_OF_WINDOW, NOW));
  }

  @Test
  void testCascadeFallThroughToFullLoad()
  {
    // Rule 1: partial { "a" } 30 days, explicit FALL_THROUGH when matcher cannot match
    PartialLoadRule partial = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        CannotMatchBehavior.FALL_THROUGH
    );
    // Rule 2: forever full load (no projections required)
    ForeverLoadRule full = new ForeverLoadRule(tier(1), null);

    // Segment with no projections. Partial rule falls through; cascade lands on full.
    DataSegment legacy = segmentWithProjections(IN_WINDOW, null);
    Assertions.assertFalse(partial.appliesTo(legacy, NOW));
    Assertions.assertTrue(full.appliesTo(legacy, NOW));

    // Segment with matching projection. Partial rule applies, cascade stops there.
    DataSegment modern = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertTrue(partial.appliesTo(modern, NOW));
  }

  @Test
  void testConstructorRejectsNullMatcher()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new PeriodPartialLoadRule(new Period("P1D"), false, tier(1), null, null, null)
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageContains("matcher must not be null")
    );
  }

  @Test
  void testPeriodSerde() throws Exception
  {
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(2),
        true,
        exact("a", "b"),
        CannotMatchBehavior.FULL_LOAD
    );
    Rule reread = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rule), Rule.class);
    Assertions.assertEquals(rule, reread);
    Assertions.assertInstanceOf(PeriodPartialLoadRule.class, reread);
  }

  @Test
  void testPeriodSerdeDefaults() throws Exception
  {
    String json = """
        {\
        "type": "loadPartialByPeriod",\
        "period": "P30D",\
        "matcher": {"type": "exactProjection", "names": ["a"]}\
        }""";
    PeriodPartialLoadRule rule = (PeriodPartialLoadRule) OBJECT_MAPPER.readValue(json, Rule.class);
    Assertions.assertEquals(CannotMatchBehavior.FULL_LOAD, rule.getOnCannotMatch());
    Assertions.assertEquals(PeriodLoadRule.DEFAULT_INCLUDE_FUTURE, rule.isIncludeFuture());
    Assertions.assertEquals(
        Map.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
        rule.getTieredReplicants()
    );
  }

  @Test
  void testUnknownOnCannotMatchValueDeserializesToFullLoadDefault() throws Exception
  {
    // Simulates an older coordinator reading a rule authored by a newer version that introduced
    // a new CannotMatchBehavior value. The rule should parse, with the unknown value falling
    // back to the constructor's default (FULL_LOAD) rather than failing deserialization.
    String json = """
        {\
        "type": "loadPartialByPeriod",\
        "period": "P30D",\
        "matcher": {"type": "exactProjection", "names": ["a"]},\
        "onCannotMatch": "SOME_FUTURE_BEHAVIOR"\
        }""";
    PeriodPartialLoadRule rule = (PeriodPartialLoadRule) OBJECT_MAPPER.readValue(json, Rule.class);
    Assertions.assertEquals(CannotMatchBehavior.FULL_LOAD, rule.getOnCannotMatch());
  }

  @Test
  void testIntervalSerde() throws Exception
  {
    IntervalPartialLoadRule rule = new IntervalPartialLoadRule(
        Intervals.of("2024-01-01/2024-02-01"),
        tier(1),
        null,
        new WildcardProjectionPartialLoadMatcher(List.of("user_*"), null),
        CannotMatchBehavior.FALL_THROUGH
    );
    Rule reread = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rule), Rule.class);
    Assertions.assertEquals(rule, reread);
    Assertions.assertInstanceOf(IntervalPartialLoadRule.class, reread);
  }

  @Test
  void testForeverSerde() throws Exception
  {
    ForeverPartialLoadRule rule = new ForeverPartialLoadRule(
        tier(1),
        null,
        exact("a"),
        CannotMatchBehavior.FULL_LOAD
    );
    Rule reread = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rule), Rule.class);
    Assertions.assertEquals(rule, reread);
    Assertions.assertInstanceOf(ForeverPartialLoadRule.class, reread);
  }

  @Test
  void testForeverAlwaysAppliesToTimeWindow()
  {
    ForeverPartialLoadRule rule = new ForeverPartialLoadRule(
        tier(1),
        null,
        exact("a"),
        CannotMatchBehavior.FULL_LOAD
    );
    DataSegment legacy = segmentWithProjections(OUT_OF_WINDOW, null);
    Assertions.assertTrue(rule.appliesTo(legacy, NOW));
  }

  @Test
  void testRunForwardsToReplicateSegment()
  {
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(2),
        null,
        exact("a"),
        null
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    RecordingHandler handler = new RecordingHandler();
    rule.run(segment, handler);
    Assertions.assertEquals(1, handler.replicateCalls);
    Assertions.assertEquals(0, handler.broadcastCalls);
    Assertions.assertEquals(0, handler.deleteCalls);
    Assertions.assertEquals(tier(2), handler.lastTierToReplicaCount);
  }

  @Test
  void testPeriodEquals()
  {
    EqualsVerifier.forClass(PeriodPartialLoadRule.class)
                  .withNonnullFields("tieredReplicants", "matcher", "onCannotMatch")
                  .withIgnoredFields("shouldSegmentBeLoaded")
                  .usingGetClass()
                  .verify();
  }

  @Test
  void testIntervalEquals()
  {
    EqualsVerifier.forClass(IntervalPartialLoadRule.class)
                  .withNonnullFields("tieredReplicants", "matcher", "onCannotMatch")
                  .withIgnoredFields("shouldSegmentBeLoaded")
                  .usingGetClass()
                  .verify();
  }

  @Test
  void testForeverEquals()
  {
    EqualsVerifier.forClass(ForeverPartialLoadRule.class)
                  .withNonnullFields("tieredReplicants", "matcher", "onCannotMatch")
                  .withIgnoredFields("shouldSegmentBeLoaded")
                  .usingGetClass()
                  .verify();
  }

  private static DataSegment segmentWithProjections(Interval interval, List<String> projections)
  {
    return DataSegment
        .builder(SegmentId.of("test", interval, DateTimes.nowUtc().toString(), new NumberedShardSpec(0, 0)))
        .loadSpec(Map.of("type", "local", "path", "/var/druid/segments/foo"))
        .projections(projections)
        .size(0)
        .build();
  }

  private static PartialLoadMatcher exact(String... names)
  {
    return new ExactProjectionPartialLoadMatcher(Arrays.asList(names));
  }

  private static Map<String, Integer> tier(int n)
  {
    return Map.of(DruidServer.DEFAULT_TIER, n);
  }

  private static class RecordingHandler implements SegmentActionHandler
  {
    int replicateCalls;
    int broadcastCalls;
    int deleteCalls;
    Map<String, Integer> lastTierToReplicaCount;

    @Override
    public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
    {
      replicateCalls++;
      lastTierToReplicaCount = tierToReplicaCount;
    }

    @Override
    public void deleteSegment(DataSegment segment)
    {
      deleteCalls++;
    }

    @Override
    public void broadcastSegment(DataSegment segment)
    {
      broadcastCalls++;
    }
  }
}
