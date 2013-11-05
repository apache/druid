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

package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.metamx.druid.processing.ComplexMetricSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

public class AdaptiveCountingAggregatorTest
{
  private void aggregate(TestAdaptiveCountingComplexMetricSelector<ICardinality> selector, AdaptiveCountingAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    ICardinality card1 = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    ICardinality card2 = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();

    card1.offer("1");
    card1.offer("2");
    card1.offer("3");

    card2.offer("3");
    card2.offer("4");
    card2.offer("5");

    final TestAdaptiveCountingComplexMetricSelector<ICardinality> selector = new TestAdaptiveCountingComplexMetricSelector<ICardinality>(
        ICardinality.class,
        new ICardinality[]{card1, card2}
    );
    AdaptiveCountingAggregator agg = new AdaptiveCountingAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    Assert.assertEquals(0, ((ICardinality) agg.get()).cardinality());
    Assert.assertEquals(0, ((ICardinality) agg.get()).cardinality());
    Assert.assertEquals(0, ((ICardinality) agg.get()).cardinality());
    aggregate(selector, agg);
    Assert.assertEquals(3, ((ICardinality) agg.get()).cardinality());
    Assert.assertEquals(3, ((ICardinality) agg.get()).cardinality());
    Assert.assertEquals(3, ((ICardinality) agg.get()).cardinality());
    aggregate(selector, agg);
    Assert.assertEquals(5, ((ICardinality) agg.get()).cardinality());
    Assert.assertEquals(5, ((ICardinality) agg.get()).cardinality());
    Assert.assertEquals(5, ((ICardinality) agg.get()).cardinality());
  }

  @Test
  public void testComparator()
  {
    ICardinality card1 = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();

    card1.offer("1");
    card1.offer("2");
    card1.offer("3");

    final TestAdaptiveCountingComplexMetricSelector<ICardinality> selector = new TestAdaptiveCountingComplexMetricSelector<ICardinality>(
        ICardinality.class,
        new ICardinality[]{card1}
    );
    AdaptiveCountingAggregator agg = new AdaptiveCountingAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    Object first = agg.get();
    agg.aggregate();

    Comparator comp = new AdaptiveCountingAggregatorFactory("null", "null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get()));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(), agg.get()));
    Assert.assertEquals(1, comp.compare(agg.get(), first));
  }

  // Provides a nice printout of error rates as a function of cardinality
  //@Test
  public void showErrorRate() throws Exception
  {
    final AdaptiveCountingAggregatorFactory aggFactory = new AdaptiveCountingAggregatorFactory("billy", "billyG");

    double error = 0.0d;
    int count = 0;

    final int[] valsToCheck = {
        10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000, 10000000, Integer.MAX_VALUE
    };

    for (int numThings : valsToCheck) {
      final ICardinality icard = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
      for (int i = 0; i < numThings; i++) {
        icard.offer(i);
      }

      final CardinalityComplexMetricSelector selector = new CardinalityComplexMetricSelector(icard);
      final AdaptiveCountingAggregator agg = new AdaptiveCountingAggregator("billy", selector);

      long startTime = System.currentTimeMillis();
      for (int i = 0; i < numThings; ++i) {
        if (i != 0 && i % 100000000 == 0) {
          ++count;
          error = computeError(error, count, i, (Long) aggFactory.finalizeComputation(agg.get()), startTime);
        }
        agg.aggregate();
      }

      ++count;
      error = computeError(error, count, numThings, (Long) aggFactory.finalizeComputation(agg.get()), startTime);
    }
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


  private static class CardinalityComplexMetricSelector implements ComplexMetricSelector<ICardinality>
  {
    private final ICardinality icard;

    private CardinalityComplexMetricSelector(ICardinality icard)
    {
      this.icard = icard;
    }

    @Override
    public Class<ICardinality> classOfObject()
    {
      return ICardinality.class;
    }

    @Override
    public ICardinality get()
    {
      return icard;
    }
  }
}
