/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.common.utils;

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
  public void testUmbrellaIntervalsSimple() throws Exception
  {
    List<Interval> intervals = Arrays.asList(
        new Interval("2011-03-03/2011-03-04"),
        new Interval("2011-01-01/2011-01-02"),
        new Interval("2011-02-01/2011-02-05"),
        new Interval("2011-02-03/2011-02-08"),
        new Interval("2011-01-01/2011-01-03"),
        new Interval("2011-03-01/2011-03-02"),
        new Interval("2011-03-05/2011-03-06"),
        new Interval("2011-02-01/2011-02-02")
    );

    Assert.assertEquals(
        new Interval("2011-01-01/2011-03-06"),
        JodaUtils.umbrellaInterval(intervals)
    );
  }

  @Test
  public void testUmbrellaIntervalsNull() throws Exception
  {
    List<Interval> intervals = Arrays.asList();
    Throwable thrown = null;
    try {
      Interval res = JodaUtils.umbrellaInterval(intervals);
    }
    catch (IllegalArgumentException e) {
      thrown = e;
    }
    Assert.assertNotNull("Empty list of intervals", thrown);
  }

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
