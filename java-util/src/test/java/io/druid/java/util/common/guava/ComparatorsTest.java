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

package io.druid.java.util.common.guava;

import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

/**
 */
public class ComparatorsTest
{
  @Test
  public void testInverse() throws Exception
  {
    Comparator<Integer> normal = Comparators.comparable();
    Comparator<Integer> inverted = Comparators.inverse(normal);

    Assert.assertEquals(-1, normal.compare(0, 1));
    Assert.assertEquals(1, normal.compare(1, 0));
    Assert.assertEquals(0, normal.compare(1, 1));
    Assert.assertEquals(1, inverted.compare(0, 1));
    Assert.assertEquals(-1, inverted.compare(1, 0));
    Assert.assertEquals(0, inverted.compare(1, 1));
  }

  @Test
  public void testInverseOverflow()
  {
    Comparator<Integer> invertedSimpleIntegerComparator = Comparators.inverse(new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return o1 - o2;
      }
    });
    Assert.assertTrue(invertedSimpleIntegerComparator.compare(0, Integer.MIN_VALUE) < 0);
  }

  @Test
  public void testIntervalsByStartThenEnd() throws Exception
  {
    Comparator<Interval> comp = Comparators.intervalsByStartThenEnd();

    Assert.assertEquals(0, comp.compare(new Interval("P1d/2011-04-02"), new Interval("2011-04-01/2011-04-02")));
    Assert.assertEquals(-1, comp.compare(new Interval("2011-03-31/2011-04-02"), new Interval("2011-04-01/2011-04-02")));
    Assert.assertEquals(1, comp.compare(new Interval("2011-04-01/2011-04-02"), new Interval("2011-03-31/2011-04-02")));
    Assert.assertEquals(1, comp.compare(new Interval("2011-04-01/2011-04-03"), new Interval("2011-04-01/2011-04-02")));
    Assert.assertEquals(-1, comp.compare(new Interval("2011-04-01/2011-04-03"), new Interval("2011-04-01/2011-04-04")));

    Interval[] intervals = new Interval[]{
        new Interval("2011-04-01T18/2011-04-02T13"),
        new Interval("2011-04-01/2011-04-03"),
        new Interval("2011-04-01/2011-04-04"),
        new Interval("2011-04-02/2011-04-04"),
        new Interval("2011-04-01/2011-04-02"),
        new Interval("2011-04-02/2011-04-03"),
        new Interval("2011-04-02/2011-04-03T06")
    };
    Arrays.sort(intervals, comp);

    Assert.assertArrayEquals(
        new Interval[]{
            new Interval("2011-04-01/2011-04-02"),
            new Interval("2011-04-01/2011-04-03"),
            new Interval("2011-04-01/2011-04-04"),
            new Interval("2011-04-01T18/2011-04-02T13"),
            new Interval("2011-04-02/2011-04-03"),
            new Interval("2011-04-02/2011-04-03T06"),
            new Interval("2011-04-02/2011-04-04"),
        },
        intervals
    );
  }

  @Test
  public void testIntervalsByEndThenStart() throws Exception
  {
    Comparator<Interval> comp = Comparators.intervalsByEndThenStart();

    Assert.assertEquals(0, comp.compare(new Interval("P1d/2011-04-02"), new Interval("2011-04-01/2011-04-02")));
    Assert.assertEquals(-1, comp.compare(new Interval("2011-04-01/2011-04-03"), new Interval("2011-04-01/2011-04-04")));
    Assert.assertEquals(1, comp.compare(new Interval("2011-04-01/2011-04-02"), new Interval("2011-04-01/2011-04-01")));
    Assert.assertEquals(-1, comp.compare(new Interval("2011-04-01/2011-04-03"), new Interval("2011-04-02/2011-04-03")));
    Assert.assertEquals(1, comp.compare(new Interval("2011-04-01/2011-04-03"), new Interval("2011-03-31/2011-04-03")));

    Interval[] intervals = new Interval[]{
        new Interval("2011-04-01T18/2011-04-02T13"),
        new Interval("2011-04-01/2011-04-03"),
        new Interval("2011-04-01/2011-04-04"),
        new Interval("2011-04-02/2011-04-04"),
        new Interval("2011-04-01/2011-04-02"),
        new Interval("2011-04-02/2011-04-03"),
        new Interval("2011-04-02/2011-04-03T06")
    };
    Arrays.sort(intervals, comp);

    Assert.assertArrayEquals(
        new Interval[]{
            new Interval("2011-04-01/2011-04-02"),
            new Interval("2011-04-01T18/2011-04-02T13"),
            new Interval("2011-04-01/2011-04-03"),
            new Interval("2011-04-02/2011-04-03"),
            new Interval("2011-04-02/2011-04-03T06"),
            new Interval("2011-04-01/2011-04-04"),
            new Interval("2011-04-02/2011-04-04")
            },
        intervals
    );
  }
}
