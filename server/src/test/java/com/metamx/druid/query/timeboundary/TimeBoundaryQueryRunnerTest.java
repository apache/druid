package com.metamx.druid.query.timeboundary;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Druids;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerTestHelper;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeBoundaryResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class TimeBoundaryQueryRunnerTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.makeQueryRunners(
        new TimeBoundaryQueryRunnerFactory()
    );
  }

  private final QueryRunner runner;

  public TimeBoundaryQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundary()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .build();

    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        runner.run(timeBoundaryQuery),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), minTime);
    Assert.assertEquals(new DateTime("2011-04-15T00:00:00.000Z"), maxTime);
  }
}
