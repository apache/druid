/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation;

import com.google.common.primitives.Longs;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.util.Comparator;

public class HistogramAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Histogram) o).count, ((Histogram) o1).count);
    }
  };

  static Histogram combineHistograms(Object lhs, Object rhs)
  {
    return ((Histogram) lhs).fold((Histogram) rhs);
  }

  private final BaseFloatColumnValueSelector selector;

  private Histogram histogram;

  public HistogramAggregator(BaseFloatColumnValueSelector selector, float[] breaks)
  {
    this.selector = selector;
    this.histogram = new Histogram(breaks);
  }

  @Override
  public void aggregate()
  {
    histogram.offer(selector.getFloat());
  }

  @Override
  public Object get()
  {
    return this.histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("HistogramAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("HistogramAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("HistogramAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
