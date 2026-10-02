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
import org.apache.druid.segment.loading.PartialBaseTableLoadSpec;
import org.apache.druid.segment.loading.PartialFullSegmentLoadSpec;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
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
        cannotMatch(),
        CannotMatchBehavior.FALL_THROUGH
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertFalse(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToMatcherDoesNotApplyDefaultStillApplies()
  {
    // LOAD_ON_DEMAND is the default, and like every behavior other than FALL_THROUGH it still applies the rule; a
    // matcher that cannot reason about the segment does not take the segment out of this tier.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        cannotMatch(),
        null
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertTrue(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToProjectionMatcherWithNoMatchingProjection()
  {
    // A projection matcher always applies: no matching projection resolves to a base-table load, not a non-match, so
    // onCannotMatch never comes into play. Asserted under FALL_THROUGH because that is where the old behavior
    // (matcher opaque -> rule skipped) would have been visible.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("nonexistent"),
        CannotMatchBehavior.FALL_THROUGH
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    Assertions.assertTrue(rule.appliesTo(segment, NOW));
  }

  @Test
  void testAppliesToProjectionAgnosticSegmentFallThrough()
  {
    // Pre-Druid-32 segment: projections == null. The projection matcher still applies, resolving to a base-table
    // load, so FALL_THROUGH does not skip the rule.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        CannotMatchBehavior.FALL_THROUGH
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, null);
    Assertions.assertTrue(rule.appliesTo(segment, NOW));
  }

  @Test
  void testRunProjectionAgnosticSegmentRoutesToBaseTablePartialLoad()
  {
    // Pre-Druid-32 segment: projections == null. The projection matcher resolves to a base-table load, so run()
    // dispatches a partial load of the segment's rows rather than falling back to whole-segment replication.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(2),
        null,
        exact("a"),
        null
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, null);
    RecordingHandler handler = new RecordingHandler();
    rule.run(segment, handler);
    Assertions.assertEquals(0, handler.replicateCalls);
    Assertions.assertEquals(1, handler.replicatePartialCalls);
    Assertions.assertEquals(PartialBaseTableLoadSpec.FINGERPRINT, handler.lastProfile.fingerprint());
    Assertions.assertEquals(
        PartialBaseTableLoadSpec.wireForm(segment.getLoadSpec(), PartialBaseTableLoadSpec.FINGERPRINT),
        handler.lastProfile.wrappedLoadSpec()
    );
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
    // Rule 1: partial cluster-group load over 30 days, explicit FALL_THROUGH when the matcher cannot match
    PartialLoadRule partial = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        cannotMatch(),
        CannotMatchBehavior.FALL_THROUGH
    );
    // Rule 2: forever full load
    ForeverLoadRule full = new ForeverLoadRule(tier(1), null);

    // Non-clustered segment: the cluster-group matcher is opaque, so the partial rule falls through and the cascade
    // lands on full.
    DataSegment unclustered = segmentWithProjections(IN_WINDOW, null);
    Assertions.assertFalse(partial.appliesTo(unclustered, NOW));
    Assertions.assertTrue(full.appliesTo(unclustered, NOW));

    // A projection rule, by contrast, always applies and so always stops the cascade.
    PartialLoadRule projectionPartial = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(1),
        null,
        exact("a"),
        CannotMatchBehavior.FALL_THROUGH
    );
    Assertions.assertTrue(projectionPartial.appliesTo(unclustered, NOW));
    Assertions.assertTrue(projectionPartial.appliesTo(segmentWithProjections(IN_WINDOW, List.of("a", "b")), NOW));
  }

  @Test
  void testConstructorRejectsNullMatcher()
  {
    DruidExceptionMatcher.assertThat(
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
    Assertions.assertEquals(CannotMatchBehavior.LOAD_ON_DEMAND, rule.getOnCannotMatch());
    Assertions.assertEquals(PeriodLoadRule.DEFAULT_INCLUDE_FUTURE, rule.isIncludeFuture());
    Assertions.assertEquals(
        Map.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
        rule.getTieredReplicants()
    );
  }

  @Test
  void testUnknownOnCannotMatchValueDeserializesToDefault() throws Exception
  {
    // Simulates an older coordinator reading a rule authored by a newer version that introduced
    // a new CannotMatchBehavior value. The rule should parse, with the unknown value falling
    // back to the constructor's default (LOAD_ON_DEMAND) rather than failing deserialization.
    String json = """
        {\
        "type": "loadPartialByPeriod",\
        "period": "P30D",\
        "matcher": {"type": "exactProjection", "names": ["a"]},\
        "onCannotMatch": "SOME_FUTURE_BEHAVIOR"\
        }""";
    PeriodPartialLoadRule rule = (PeriodPartialLoadRule) OBJECT_MAPPER.readValue(json, Rule.class);
    Assertions.assertEquals(CannotMatchBehavior.LOAD_ON_DEMAND, rule.getOnCannotMatch());
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
  void testRunWithMatchRoutesToReplicateSegmentPartially()
  {
    // Matcher resolves to a non-empty set on the segment, so run() routes through the partial-load handler with a
    // PartialLoadProfile carrying the resolved set + fingerprint.
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
    Assertions.assertEquals(0, handler.replicateCalls);
    Assertions.assertEquals(1, handler.replicatePartialCalls);
    Assertions.assertEquals(tier(2), handler.lastTierToReplicaCount);
    Assertions.assertNotNull(handler.lastProfile);
    Assertions.assertNotNull(handler.lastProfile.wrappedLoadSpec());
    Assertions.assertEquals("partialProjection", handler.lastProfile.wrappedLoadSpec().get("type"));
    Assertions.assertEquals(List.of("a"), handler.lastProfile.wrappedLoadSpec().get("projections"));
    Assertions.assertTrue(handler.lastProfile.fingerprint().startsWith("v1:"));
  }

  @Test
  void testRunWithLoadOnDemandFallbackRoutesToReplicateSegment()
  {
    // Matcher does not apply and onCannotMatch is LOAD_ON_DEMAND, so run() dispatches the segment with no partial
    // wrapper at all — on a virtual-storage historical its bundles are then fetched as queries touch them.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(2),
        null,
        cannotMatch(),
        CannotMatchBehavior.LOAD_ON_DEMAND
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    RecordingHandler handler = new RecordingHandler();
    rule.run(segment, handler);
    Assertions.assertEquals(1, handler.replicateCalls);
    Assertions.assertEquals(0, handler.replicatePartialCalls);
    Assertions.assertEquals(tier(2), handler.lastTierToReplicaCount);
    Assertions.assertNull(handler.lastProfile);
  }

  @Test
  void testRunWithBaseLoadFallbackRoutesToBaseTablePartialLoad()
  {
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(2),
        null,
        cannotMatch(),
        CannotMatchBehavior.BASE_LOAD
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    RecordingHandler handler = new RecordingHandler();
    rule.run(segment, handler);
    Assertions.assertEquals(0, handler.replicateCalls);
    Assertions.assertEquals(1, handler.replicatePartialCalls);
    Assertions.assertEquals(PartialBaseTableLoadSpec.FINGERPRINT, handler.lastProfile.fingerprint());
    Assertions.assertEquals(
        PartialBaseTableLoadSpec.wireForm(segment.getLoadSpec(), PartialBaseTableLoadSpec.FINGERPRINT),
        handler.lastProfile.wrappedLoadSpec()
    );
  }

  @Test
  void testRunWithFullLoadFallbackRoutesToFullSegmentPartialLoad()
  {
    // FULL_LOAD means every bundle resident, which needs the partial-load path: dispatching without a wrapper would
    // only make the segment available on demand.
    PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        new Period("P30D"),
        false,
        tier(2),
        null,
        cannotMatch(),
        CannotMatchBehavior.FULL_LOAD
    );
    DataSegment segment = segmentWithProjections(IN_WINDOW, List.of("a", "b"));
    RecordingHandler handler = new RecordingHandler();
    rule.run(segment, handler);
    Assertions.assertEquals(0, handler.replicateCalls);
    Assertions.assertEquals(1, handler.replicatePartialCalls);
    Assertions.assertEquals(PartialFullSegmentLoadSpec.FINGERPRINT, handler.lastProfile.fingerprint());
    Assertions.assertEquals(
        PartialFullSegmentLoadSpec.wireForm(segment.getLoadSpec(), PartialFullSegmentLoadSpec.FINGERPRINT),
        handler.lastProfile.wrappedLoadSpec()
    );
  }

  @Test
  void testBaseLoadAndFullLoadFingerprintsDiffer()
  {
    // Otherwise the coordinator could not tell a rule swap between the two apart, and would never re-issue the load.
    Assertions.assertNotEquals(PartialBaseTableLoadSpec.FINGERPRINT, PartialFullSegmentLoadSpec.FINGERPRINT);
  }

  @Test
  void testAllCannotMatchBehaviorsRoundTrip() throws Exception
  {
    for (CannotMatchBehavior behavior : CannotMatchBehavior.values()) {
      ForeverPartialLoadRule rule = new ForeverPartialLoadRule(tier(1), null, exact("a"), behavior);
      Rule reread = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rule), Rule.class);
      Assertions.assertEquals(rule, reread, "round trip failed for " + behavior);
      Assertions.assertEquals(behavior, ((ForeverPartialLoadRule) reread).getOnCannotMatch());
    }
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

  /**
   * A matcher that cannot match any segment this test builds, so the rule's {@link CannotMatchBehavior} decides.
   * Projection matchers no longer serve this purpose: they always apply, falling back to a base-table load when none
   * of their projections are present. A cluster-group matcher is opaque to the non-clustered segments here.
   */
  private static PartialLoadMatcher cannotMatch()
  {
    return new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("tenant", "acme")), null);
  }

  private static Map<String, Integer> tier(int n)
  {
    return Map.of(DruidServer.DEFAULT_TIER, n);
  }

  private static class RecordingHandler implements SegmentActionHandler
  {
    int replicateCalls;
    int replicatePartialCalls;
    int broadcastCalls;
    int deleteCalls;
    Map<String, Integer> lastTierToReplicaCount;
    PartialLoadProfile lastProfile;

    @Override
    public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
    {
      replicateCalls++;
      lastTierToReplicaCount = tierToReplicaCount;
    }

    @Override
    public void replicateSegmentPartially(
        DataSegment segment,
        PartialLoadProfile profile,
        Map<String, Integer> tierToReplicaCount
    )
    {
      replicatePartialCalls++;
      lastProfile = profile;
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
