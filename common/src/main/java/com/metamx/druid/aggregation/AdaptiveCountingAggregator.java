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
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.primitives.Longs;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ComplexMetricSelector;

import java.io.IOException;
import java.util.Comparator;

public class AdaptiveCountingAggregator implements Aggregator
{
  private static final Logger log = new Logger(AdaptiveCountingAggregator.class);

  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((ICardinality) o).cardinality(), ((ICardinality) o1).cardinality());
    }
  };

  static Object combineValues(Object lhs, Object rhs)
  {
    try {
      return ((ICardinality) lhs).merge((ICardinality) rhs);
    }
    catch (CardinalityMergeException e) {
      return lhs;
    }
  }

  private final ComplexMetricSelector<ICardinality> selector;
  private final String name;
  private ICardinality card;

  public AdaptiveCountingAggregator(String name, ComplexMetricSelector<ICardinality> selector)
  {
    this.name = name;
    this.selector = selector;
    this.card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
  }

  public AdaptiveCountingAggregator(String name, ComplexMetricSelector<ICardinality> selector, ICardinality card)
  {
    this.name = name;
    this.selector = selector;
    this.card = card;
  }

  @Override
  public void aggregate()
  {
    ICardinality valueToAgg = selector.get();
    try {
      ICardinality mergedCardinality = card.merge(valueToAgg);
      card = mergedCardinality;
    }
    catch (CardinalityMergeException e) {
      throw new RuntimeException("Failed to aggregate: " + e);
    }
  }

  @Override
  public void reset()
  {
    card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
  }

  @Override
  public Object get()
  {
    return card;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("AdaptiveCountingAggregator does not support getFloat()");
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public Aggregator clone()
  {
    try {
      ICardinality cardinality = new AdaptiveCounting(card.getBytes());
      return new AdaptiveCountingAggregator(name, selector, cardinality);
    }
    catch (IOException e) {
      return null;
    }
  }

  @Override
  public void close()
  {
    card = null;
  }
}
