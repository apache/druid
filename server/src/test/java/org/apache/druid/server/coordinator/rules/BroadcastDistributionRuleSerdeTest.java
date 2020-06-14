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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class BroadcastDistributionRuleSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Parameters(name = "{0}")
  public static List<Object> constructorFeeder()
  {
    return Arrays.asList(
        new ForeverBroadcastDistributionRule(),
        new IntervalBroadcastDistributionRule(Intervals.of("0/1000")),
        new PeriodBroadcastDistributionRule(new Period(1000), null)
    );
  }

  @Parameter
  public Rule testRule;

  @Test
  public void testSerde() throws IOException
  {
    String json = MAPPER.writeValueAsString(testRule);
    Rule fromJson = MAPPER.readValue(json, Rule.class);
    Assert.assertEquals(testRule, fromJson);
  }
}
