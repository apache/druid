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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class BroadcastDistributionRuleSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Parameterized.Parameters
  public static List<Object[]> constructorFeeder()
  {
    return Lists.newArrayList(
        new Object[]{new ForeverBroadcastDistributionRule()},
        new Object[]{new IntervalBroadcastDistributionRule(Intervals.of("0/1000"))},
        new Object[]{new PeriodBroadcastDistributionRule(new Period(1000), null)}
    );
  }

  private final Rule testRule;

  public BroadcastDistributionRuleSerdeTest(Rule testRule)
  {
    this.testRule = testRule;
  }

  @Test
  public void testSerde() throws IOException
  {
    final List<Rule> rules = Collections.singletonList(testRule);
    final String json = MAPPER.writeValueAsString(rules);
    final List<Rule> fromJson = MAPPER.readValue(json, new TypeReference<List<Rule>>(){});
    Assert.assertEquals(rules, fromJson);
  }
}
