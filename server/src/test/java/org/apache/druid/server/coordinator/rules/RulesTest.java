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

import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class RulesTest
{
  // Load rules
  private static final PeriodLoadRule LOAD_PT1H = new PeriodLoadRule(
      new Period("PT1H"), true, null, null
  );
  private static final PeriodLoadRule LOAD_PT1H_EXLUDE_FUTURE = new PeriodLoadRule(
      new Period("PT1H"), false, null, null
  );
  private static final PeriodLoadRule LOAD_P3M = new PeriodLoadRule(
      new Period("P3M"), true, null, null
  );
  private static final IntervalLoadRule LOAD_2020_2023 = new IntervalLoadRule(
      Intervals.of("2020/2023"), null, null
  );
  private static final IntervalLoadRule LOAD_2021_2022 = new IntervalLoadRule(
      Intervals.of("2021/2022"), null, null
  );
  private static final IntervalLoadRule LOAD_1980_2050 = new IntervalLoadRule(
      Intervals.of("1980/2050"), null, null
  );
  private static final ForeverLoadRule LOAD_FOREVER = new ForeverLoadRule(null, null);

  // Drop rules
  private static final PeriodDropBeforeRule DROP_BEFORE_P3M = new PeriodDropBeforeRule(new Period("P3M"));
  private static final PeriodDropBeforeRule DROP_BEFORE_P6M = new PeriodDropBeforeRule(new Period("P6M"));
  private static final PeriodDropRule DROP_P1M = new PeriodDropRule(new Period("P1M"), true);
  private static final PeriodDropRule DROP_P2M = new PeriodDropRule(new Period("P2M"), true);
  private static final IntervalDropRule DROP_2000_2020 = new IntervalDropRule(Intervals.of("2000/2020"));
  private static final IntervalDropRule DROP_2010_2020 = new IntervalDropRule(Intervals.of("2010/2020"));
  private static final ForeverDropRule DROP_FOREVER = new ForeverDropRule();

  // Broadcast rules
  private static final PeriodBroadcastDistributionRule BROADCAST_PT1H = new PeriodBroadcastDistributionRule(
      new Period("PT1H"), true
  );
  private static final PeriodBroadcastDistributionRule BROADCAST_PT1H_EXCLUDE_FUTURE = new PeriodBroadcastDistributionRule(
      new Period("PT1H"), false
  );
  private static final PeriodBroadcastDistributionRule BROADCAST_PT2H = new PeriodBroadcastDistributionRule(
      new Period("PT2H"), true
  );
  private static final IntervalBroadcastDistributionRule BROADCAST_2000_2050 = new IntervalBroadcastDistributionRule(
      Intervals.of("2000/2050")
  );
  private static final IntervalBroadcastDistributionRule BROADCAST_2010_2020 = new IntervalBroadcastDistributionRule(
      Intervals.of("2010/2020")
  );
  private static final ForeverBroadcastDistributionRule BROADCAST_FOREVER = new ForeverBroadcastDistributionRule();

  private final List<Rule> rules;
  private final boolean isInvalid;
  private final Rule invalidRule1;
  private final Rule invalidRule2;

  public RulesTest(final List<Rule> rules, final boolean isInvalid, final Rule invalidRule1, final Rule invalidRule2)
  {
    this.rules = rules;
    this.isInvalid = isInvalid;
    this.invalidRule1 = invalidRule1;
    this.invalidRule2 = invalidRule2;
  }

  @Parameterized.Parameters
  public static Object[] inputsAndExpectations()
  {
    return new Object[][] {
        // Invalid rules
        {getRules(LOAD_PT1H, LOAD_2021_2022, LOAD_FOREVER, LOAD_P3M), true, LOAD_FOREVER, LOAD_P3M},
        {getRules(LOAD_PT1H, LOAD_P3M, LOAD_PT1H_EXLUDE_FUTURE, LOAD_1980_2050), true, LOAD_PT1H, LOAD_PT1H_EXLUDE_FUTURE},
        {getRules(LOAD_PT1H, LOAD_P3M, LOAD_2020_2023, LOAD_P3M), true, LOAD_P3M, LOAD_P3M},
        {getRules(LOAD_2020_2023, LOAD_2021_2022, LOAD_P3M, LOAD_FOREVER), true, LOAD_2020_2023, LOAD_2021_2022},
        {getRules(LOAD_P3M, LOAD_2021_2022, LOAD_PT1H, LOAD_2020_2023), true, LOAD_P3M, LOAD_PT1H},
        {getRules(LOAD_P3M, LOAD_2021_2022, LOAD_FOREVER, LOAD_2020_2023), true, LOAD_FOREVER, LOAD_2020_2023},
        {getRules(DROP_BEFORE_P3M, DROP_P1M, DROP_BEFORE_P6M, DROP_FOREVER), true, DROP_BEFORE_P3M, DROP_BEFORE_P6M},
        {getRules(DROP_P2M, DROP_P1M, DROP_FOREVER), true, DROP_P2M, DROP_P1M},
        {getRules(DROP_2000_2020, DROP_P1M, DROP_P2M, DROP_2010_2020), true, DROP_2000_2020, DROP_2010_2020},
        {getRules(DROP_P1M, DROP_FOREVER, DROP_P2M), true, DROP_FOREVER, DROP_P2M},
        {getRules(BROADCAST_2000_2050, BROADCAST_PT1H, BROADCAST_2010_2020, BROADCAST_FOREVER), true, BROADCAST_2000_2050, BROADCAST_2010_2020},
        {getRules(BROADCAST_PT2H, BROADCAST_2000_2050, BROADCAST_PT1H, BROADCAST_FOREVER), true, BROADCAST_PT2H, BROADCAST_PT1H},
        {getRules(BROADCAST_PT1H, BROADCAST_PT1H_EXCLUDE_FUTURE, BROADCAST_FOREVER), true, BROADCAST_PT1H, BROADCAST_PT1H_EXCLUDE_FUTURE},
        {getRules(BROADCAST_PT1H, BROADCAST_PT2H, BROADCAST_2010_2020, BROADCAST_FOREVER, BROADCAST_2000_2050), true, BROADCAST_FOREVER, BROADCAST_2000_2050},
        {getRules(LOAD_PT1H, LOAD_1980_2050, LOAD_P3M, LOAD_FOREVER, DROP_FOREVER), true, LOAD_FOREVER, DROP_FOREVER},

        // Valid rules
        {null, false, null, null},
        {getRules(), false, null, null},
        {getRules(LOAD_FOREVER), false, null, null},
        {getRules(LOAD_PT1H_EXLUDE_FUTURE, LOAD_PT1H, LOAD_P3M, LOAD_FOREVER), false, null, null},
        {getRules(DROP_2010_2020, DROP_2000_2020, DROP_P1M, DROP_P2M, DROP_BEFORE_P3M, DROP_FOREVER), false, null, null},
        {getRules(BROADCAST_PT1H, BROADCAST_PT2H, BROADCAST_2010_2020, BROADCAST_2000_2050, BROADCAST_FOREVER), false, null, null},
        {getRules(DROP_BEFORE_P6M, DROP_P1M, DROP_BEFORE_P3M, LOAD_2020_2023, DROP_FOREVER), false, null, null},
        {getRules(DROP_2000_2020, LOAD_PT1H, BROADCAST_2000_2050, LOAD_1980_2050, LOAD_P3M, LOAD_FOREVER), false, null, null},
    };
  }

  private static ArrayList<Rule> getRules(Rule... rules)
  {
    return new ArrayList<>(Arrays.asList(rules));
  }

  @Test
  public void testValidateRules()
  {
    if (this.isInvalid) {
      DruidExceptionMatcher.invalidInput().expectMessageContains(
          StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].",
                             invalidRule1, invalidRule2
          )
      ).assertThrowsAndMatches(() -> Rules.validateRules(rules));
    } else {
      Rules.validateRules(rules);
    }
  }
}
