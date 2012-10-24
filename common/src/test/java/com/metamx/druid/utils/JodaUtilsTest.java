package com.metamx.druid.utils;

import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class JodaUtilsTest
{
  @Test
  public void testCondenseIntervalsSimple() throws Exception
  {
    List<Interval> intervals = Arrays.asList(
        new Interval("2011-01-01/2011-01-02"),
        new Interval("2011-01-02/2011-01-03"),
        new Interval("2011-02-01/2011-02-05"),
        new Interval("2011-02-01/2011-02-02"),
        new Interval("2011-02-03/2011-02-08"),
        new Interval("2011-03-01/2011-03-02"),
        new Interval("2011-03-03/2011-03-04"),
        new Interval("2011-03-05/2011-03-06")
    );

    Assert.assertEquals(
        Arrays.asList(
            new Interval("2011-01-01/2011-01-03"),
            new Interval("2011-02-01/2011-02-08"),
            new Interval("2011-03-01/2011-03-02"),
            new Interval("2011-03-03/2011-03-04"),
            new Interval("2011-03-05/2011-03-06")
        ),
        JodaUtils.condenseIntervals(intervals)
    );
  }

  @Test
  public void testCondenseIntervalsMixedUp() throws Exception
  {
    List<Interval> intervals = Arrays.asList(
        new Interval("2011-01-01/2011-01-02"),
        new Interval("2011-01-02/2011-01-03"),
        new Interval("2011-02-01/2011-02-05"),
        new Interval("2011-02-01/2011-02-02"),
        new Interval("2011-02-03/2011-02-08"),
        new Interval("2011-03-01/2011-03-02"),
        new Interval("2011-03-03/2011-03-04"),
        new Interval("2011-03-05/2011-03-06")
    );

    for (int i = 0; i < 20; ++i) {
      Collections.shuffle(intervals);
      Assert.assertEquals(
          Arrays.asList(
              new Interval("2011-01-01/2011-01-03"),
              new Interval("2011-02-01/2011-02-08"),
              new Interval("2011-03-01/2011-03-02"),
              new Interval("2011-03-03/2011-03-04"),
              new Interval("2011-03-05/2011-03-06")
          ),
          JodaUtils.condenseIntervals(intervals)
      );
    }
  }
}
