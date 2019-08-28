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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class PeriodDropBeforeRuleTest
{
  private static final DataSegment.Builder BUILDER = DataSegment.builder()
                                                                .dataSource("test")
                                                                .version(DateTimes.of("2012-12-31T01:00:00").toString())
                                                                .shardSpec(NoneShardSpec.instance());

  @Test
  public void testSerde() throws Exception
  {
    PeriodDropBeforeRule rule = new PeriodDropBeforeRule(
        new Period("P1D")
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

    Assert.assertEquals(rule.getPeriod(), ((PeriodDropBeforeRule) reread).getPeriod());
  }

  @Test
  public void testDropBefore()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodDropBeforeRule rule = new PeriodDropBeforeRule(
        new Period("P1D")
    );

    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusDays(3), now.minusDays(2))).build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusDays(2), now.minusDays(1))).build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusDays(1), now)).build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            BUILDER.interval(new Interval(now, now.plusDays(1))).build(),
            now
        )
    );
  }
}
