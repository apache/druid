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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BroadcastDistributionRuleSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Parameterized.Parameters
  public static List<Object[]> constructorFeeder()
  {
    final List<Object[]> params = Lists.newArrayList(
        new Object[]{new ForeverBroadcastDistributionRule(ImmutableList.of("large_source1", "large_source2"))},
        new Object[]{new ForeverBroadcastDistributionRule(ImmutableList.of())},
        new Object[]{new ForeverBroadcastDistributionRule(null)},
        new Object[]{new IntervalBroadcastDistributionRule(new Interval("0/1000"), ImmutableList.of("large_source"))},
        new Object[]{new IntervalBroadcastDistributionRule(new Interval("0/1000"), ImmutableList.of())},
        new Object[]{new IntervalBroadcastDistributionRule(new Interval("0/1000"), null)},
        new Object[]{new PeriodBroadcastDistributionRule(new Period(1000), ImmutableList.of("large_source"))},
        new Object[]{new PeriodBroadcastDistributionRule(new Period(1000), ImmutableList.of())},
        new Object[]{new PeriodBroadcastDistributionRule(new Period(1000), null)}
    );
    return params;
  }

  private final Rule testRule;

  public BroadcastDistributionRuleSerdeTest(Rule testRule)
  {
    this.testRule = testRule;
  }

  @Test
  public void testSerde() throws IOException
  {
    final List<Rule> rules = Lists.newArrayList(testRule);
    final String json = MAPPER.writeValueAsString(rules);
    final List<Rule> fromJson = MAPPER.readValue(json, new TypeReference<List<Rule>>(){});
    assertEquals(rules, fromJson);
  }
}
