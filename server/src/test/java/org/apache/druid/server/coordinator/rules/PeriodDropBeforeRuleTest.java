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
  private static final DataSegment.Builder builder = DataSegment.builder()
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
            builder.interval(new Interval(now.minusDays(3), now.minusDays(2))).build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(new Interval(now.minusDays(2), now.minusDays(1))).build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            builder.interval(new Interval(now.minusDays(1), now)).build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            builder.interval(new Interval(now, now.plusDays(1))).build(),
            now
        )
    );
  }
}
