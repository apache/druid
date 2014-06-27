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

package io.druid.query.aggregation.histogram;

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

public class ApproximateHistogramAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((ApproximateHistogram) o).count(), ((ApproximateHistogram) o1).count());
    }
  };

  static Object combineHistograms(Object lhs, Object rhs)
  {
    return ((ApproximateHistogram) lhs).foldFast((ApproximateHistogram) rhs);
  }

  private final String name;
  private final FloatColumnSelector selector;
  private final int resolution;
  private final float lowerLimit;
  private final float upperLimit;

  private ApproximateHistogram histogram;

  public ApproximateHistogramAggregator(
      String name,
      FloatColumnSelector selector,
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.name = name;
    this.selector = selector;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.histogram = new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }

  @Override
  public void aggregate()
  {
    histogram.offer(selector.get());
  }

  @Override
  public void reset()
  {
    this.histogram = new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }

  @Override
  public Object get()
  {
    return histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("ApproximateHistogramAggregator does not support getFloat()");
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
