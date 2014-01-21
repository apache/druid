/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import gnu.trove.map.hash.TIntByteHashMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

public class HyperloglogAggregatorTest
{
  @Test
  public void testAggregate()
  {
    final TestHllComplexMetricSelector selector = new TestHllComplexMetricSelector();
    final HyperloglogAggregatorFactory aggFactory = new HyperloglogAggregatorFactory("billy", "billyG");
    final HyperloglogAggregator agg = new HyperloglogAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    Assert.assertEquals(0L, aggFactory.finalizeComputation(agg.get()));
    Assert.assertEquals(0L, aggFactory.finalizeComputation(agg.get()));
    Assert.assertEquals(0L, aggFactory.finalizeComputation(agg.get()));

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(3L, aggFactory.finalizeComputation(agg.get()));
    Assert.assertEquals(3L, aggFactory.finalizeComputation(agg.get()));
    Assert.assertEquals(3L, aggFactory.finalizeComputation(agg.get()));

    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(5L, aggFactory.finalizeComputation(agg.get()));
    Assert.assertEquals(5L, aggFactory.finalizeComputation(agg.get()));
  }

  @Test
  public void testComparator()
  {
    final TestHllComplexMetricSelector selector = new TestHllComplexMetricSelector();
    final Comparator comp = new HyperloglogAggregatorFactory("billy", "billyG").getComparator();
    final HyperloglogAggregator agg = new HyperloglogAggregator("billy", selector);

    Object first = new TIntByteHashMap((TIntByteHashMap) agg.get());
    agg.aggregate();

    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(), agg.get()));
    Assert.assertEquals(1, comp.compare(agg.get(), first));
  }

  @Test
  public void testHighCardinalityAggregate()
  {
    final TestHllComplexMetricSelector selector = new TestHllComplexMetricSelector();
    final HyperloglogAggregatorFactory aggFactory = new HyperloglogAggregatorFactory("billy", "billyG");
    final HyperloglogAggregator agg = new HyperloglogAggregator("billy", selector);

    final int card = 100000;

    for (int i = 0; i < card; i++) {
      aggregate(selector, agg);
    }

    Assert.assertEquals(99443L, aggFactory.finalizeComputation(agg.get()));
  }

  // Provides a nice printout of error rates as a function of cardinality
  //@Test
  public void benchmarkAggregation() throws Exception
  {
    final TestHllComplexMetricSelector selector = new TestHllComplexMetricSelector();
    final HyperloglogAggregatorFactory aggFactory = new HyperloglogAggregatorFactory("billy", "billyG");

    double error = 0.0d;
    int count = 0;

    final int[] valsToCheck = {
        10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000, 10000000, Integer.MAX_VALUE
    };

    for (int numThings : valsToCheck) {
      long startTime = System.currentTimeMillis();
      final HyperloglogAggregator agg = new HyperloglogAggregator("billy", selector);

      for (int i = 0; i < numThings; ++i) {
        if (i != 0 && i % 100000000 == 0) {
          ++count;
          error = computeError(error, count, i, (Long) aggFactory.finalizeComputation(agg.get()), startTime);
        }
        aggregate(selector, agg);
      }

      ++count;
      error = computeError(error, count, numThings, (Long) aggFactory.finalizeComputation(agg.get()), startTime);
    }
  }

  //@Test
  public void benchmarkCombine() throws Exception
  {
    int count;
    long totalTime = 0;

    final TestHllComplexMetricSelector selector = new TestHllComplexMetricSelector();
    TIntByteHashMap combined = new TIntByteHashMap();

    for (count = 0; count < 1000000; ++count) {
      final HyperloglogAggregator agg = new HyperloglogAggregator("billy", selector);
      aggregate(selector, agg);

      long start = System.nanoTime();
      combined = (TIntByteHashMap) HyperloglogAggregator.combine(agg.get(), combined);
      totalTime += System.nanoTime() - start;
    }
    System.out.printf("benchmarkCombine took %d ms%n", totalTime / 1000000);
  }

  private double computeError(double error, int count, long exactValue, long estimatedValue, long startTime)
  {
    final double errorThisTime = Math.abs((double) exactValue - estimatedValue) / exactValue;

    error += errorThisTime;

    System.out.printf(
        "%,d ==? %,d in %,d millis. actual error[%,f%%], avg. error [%,f%%]%n",
        exactValue,
        estimatedValue,
        System.currentTimeMillis() - startTime,
        100 * errorThisTime,
        (error / count) * 100
    );
    return error;
  }

  private void aggregate(TestHllComplexMetricSelector selector, HyperloglogAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }
}
