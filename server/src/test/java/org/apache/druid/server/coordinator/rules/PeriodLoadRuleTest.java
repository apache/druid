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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.client.DruidServer;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class PeriodLoadRuleTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private static final DataSegment.Builder BUILDER = DataSegment
      .builder()
      .dataSource("test")
      .version(DateTimes.nowUtc().toString())
      .shardSpec(NoneShardSpec.instance())
      .size(0);

  @Test
  public void testAppliesToAll()
  {
    DateTime now = DateTimes.of("2013-01-01");
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P5000Y"),
        false,
        ImmutableMap.of("", 0),
        null
    );

    Assert.assertTrue(rule.appliesTo(BUILDER.interval(Intervals.of("2012-01-01/2012-12-31")).build(), now));
    Assert.assertTrue(rule.appliesTo(BUILDER.interval(Intervals.of("1000-01-01/2012-12-31")).build(), now));
    Assert.assertTrue(rule.appliesTo(BUILDER.interval(Intervals.of("0500-01-01/2100-12-31")).build(), now));
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P1M"),
        false,
        ImmutableMap.of("", 0),
        null
    );

    Assert.assertTrue(rule.appliesTo(BUILDER.interval(new Interval(now.minusWeeks(1), now)).build(), now));
    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusDays(1), now.plusDays(1)))
                   .build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.plusDays(1), now.plusDays(2)))
                   .build(),
            now
        )
    );
  }

  @Test
  public void testAppliesToPartialOverlap()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P1M"),
        false,
        ImmutableMap.of("", 0),
        null
    );

    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusWeeks(1), now.plusWeeks(1))).build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(
                new Interval(now.minusMonths(1).minusWeeks(1), now.minusMonths(1).plusWeeks(1))
            ).build(),
            now
        )
    );
  }

  @Test
  public void testIncludeFuture()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodLoadRule includeFutureRule = new PeriodLoadRule(
        new Period("P2D"),
        true,
        ImmutableMap.of("", 0),
        null
    );
    PeriodLoadRule notIncludeFutureRule = new PeriodLoadRule(
        new Period("P2D"),
        false,
        ImmutableMap.of("", 0),
        null
    );

    Assert.assertTrue(
        includeFutureRule.appliesTo(
            BUILDER.interval(new Interval(now.plusDays(1), now.plusDays(2))).build(),
            now
        )
    );
    Assert.assertFalse(
        notIncludeFutureRule.appliesTo(
            BUILDER.interval(new Interval(now.plusDays(1), now.plusDays(2))).build(),
            now
        )
    );
  }

  /**
   * test serialize/deserilize null values of {@link PeriodLoadRule#tieredReplicants} and {@link PeriodLoadRule#includeFuture}
   */
  @Test
  public void testSerdeNull() throws Exception
  {
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P1D"), null, null, null
    );

    Rule reread = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rule), Rule.class);

    Assert.assertEquals(rule.getPeriod(), ((PeriodLoadRule) reread).getPeriod());
    Assert.assertEquals(rule.isIncludeFuture(), ((PeriodLoadRule) reread).isIncludeFuture());
    Assert.assertEquals(PeriodLoadRule.DEFAULT_INCLUDE_FUTURE, rule.isIncludeFuture());
    Assert.assertEquals(rule.getTieredReplicants(), ((PeriodLoadRule) reread).getTieredReplicants());
    Assert.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), rule.getTieredReplicants());
  }

  /**
   * test mapping null values of {@link PeriodLoadRule#tieredReplicants} and {@link PeriodLoadRule#includeFuture}
   */
  @Test
  public void testMappingNull() throws Exception
  {
    String inputJson = "{\n"
                       + "      \"period\": \"P1D\",\n"
                       + "      \"type\": \"loadByPeriod\"\n"
                       + "    }";
    String expectedJson = "{\n"
                          + "      \"period\": \"P1D\",\n"
                          + "      \"includeFuture\": " + PeriodLoadRule.DEFAULT_INCLUDE_FUTURE + ",\n"
                          + "      \"type\": \"loadByPeriod\"\n"
                          + "    }";
    PeriodLoadRule inputPeriodLoadRule = OBJECT_MAPPER.readValue(inputJson, PeriodLoadRule.class);
    PeriodLoadRule expectedPeriodLoadRule = OBJECT_MAPPER.readValue(expectedJson, PeriodLoadRule.class);
    Assert.assertEquals(expectedPeriodLoadRule.getTieredReplicants(), inputPeriodLoadRule.getTieredReplicants());
    Assert.assertEquals(expectedPeriodLoadRule.getPeriod(), inputPeriodLoadRule.getPeriod());
    Assert.assertEquals(expectedPeriodLoadRule.isIncludeFuture(), inputPeriodLoadRule.isIncludeFuture());
  }

  @Test
  public void testCreatingNegativeTieredReplicants()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () ->
            new PeriodLoadRule(
                Period.days(1),
                true,
                ImmutableMap.of(DruidServer.DEFAULT_TIER, -1),
                true
            )
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains(
            "Invalid number of replicas for tier [_default_tier]. Value [-1] must be positive."
        )
    );
  }

  @Test
  public void testNullReplicantValue()
  {
    // Immutable map does not allow null values
    Map<String, Integer> tieredReplicants = new HashMap<>();
    tieredReplicants.put("tier", null);

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () ->
            new PeriodLoadRule(
                Period.days(1),
                true,
                tieredReplicants,
                true
            )
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains(
            "Invalid number of replicas for tier [tier]. Value must not be null."
        )
    );
  }


  @Test
  public void testShouldCreateDefaultTier() throws Exception
  {
    String inputJson = "      {\n"
                       + "      \"period\": \"P1D\",\n"
                       + "      \"includeFuture\": " + PeriodLoadRule.DEFAULT_INCLUDE_FUTURE + ",\n"
                       + "      \"type\": \"loadByPeriod\"\n"
                       + "    }";
    PeriodLoadRule inputPeriodLoadRule = OBJECT_MAPPER.readValue(inputJson, PeriodLoadRule.class);
    Assert.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), inputPeriodLoadRule.getTieredReplicants());
  }

  @Test
  public void testUseDefaultTierAsTrueShouldCreateDefaultTier() throws Exception
  {
    String inputJson = "      {\n"
                       + "      \"period\": \"P1D\",\n"
                       + "      \"includeFuture\": " + PeriodLoadRule.DEFAULT_INCLUDE_FUTURE + ",\n"
                       + "      \"useDefaultTierForNull\": \"true\",\n"
                       + "      \"type\": \"loadByPeriod\"\n"
                       + "    }";
    PeriodLoadRule inputPeriodLoadRule = OBJECT_MAPPER.readValue(inputJson, PeriodLoadRule.class);
    Assert.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), inputPeriodLoadRule.getTieredReplicants());
  }

  @Test
  public void testUseDefaultTierAsFalseShouldCreateEmptyMap() throws Exception
  {
    String inputJson = "    {\n"
                       + "     \"period\": \"P1D\",\n"
                       + "     \"includeFuture\": " + PeriodLoadRule.DEFAULT_INCLUDE_FUTURE + ",\n"
                       + "     \"useDefaultTierForNull\": \"false\",\n"
                       + "     \"type\": \"loadByPeriod\"\n"
                       + "  }";
    PeriodLoadRule inputPeriodLoadRule = OBJECT_MAPPER.readValue(inputJson, PeriodLoadRule.class);
    Assert.assertEquals(ImmutableMap.of(), inputPeriodLoadRule.getTieredReplicants());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PeriodLoadRule.class)
                  .withNonnullFields("tieredReplicants")
                  .withIgnoredFields("shouldSegmentBeLoaded")
                  .usingGetClass()
                  .verify();
  }
}
