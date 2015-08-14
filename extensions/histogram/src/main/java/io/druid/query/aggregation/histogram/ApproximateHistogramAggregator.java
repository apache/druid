/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.aggregation.histogram;

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

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
  private final ObjectColumnSelector selector;
  private final boolean ignoreNullValue;
  private final int resolution;
  private final float lowerLimit;
  private final float upperLimit;

  private ApproximateHistogram histogram;

  public ApproximateHistogramAggregator(
      String name,
      ObjectColumnSelector selector,
      boolean ignoreNullValue,
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.name = name;
    this.selector = selector;
    this.ignoreNullValue = ignoreNullValue;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.histogram = new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }

  @Override
  public void aggregate()
  {
    Object val = selector.get();

    if(val != null || !ignoreNullValue) {
      histogram.offer(ApproximateHistogramAggregatorFactory.convertToFloat(val));
    }
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
  public long getLong()
  {
    throw new UnsupportedOperationException("ApproximateHistogramAggregator does not support getLong()");
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
