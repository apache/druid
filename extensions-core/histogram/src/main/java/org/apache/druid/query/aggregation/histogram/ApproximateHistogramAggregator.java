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

package org.apache.druid.query.aggregation.histogram;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.util.Comparator;

public class ApproximateHistogramAggregator implements Aggregator
{
  public static final Comparator<ApproximateHistogram> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingLong(ApproximateHistogram::count));

  static ApproximateHistogram combineHistograms(Object lhs, Object rhs)
  {
    return ((ApproximateHistogram) lhs).foldFast((ApproximateHistogram) rhs);
  }

  private final BaseFloatColumnValueSelector selector;

  private ApproximateHistogram histogram;

  public ApproximateHistogramAggregator(
      BaseFloatColumnValueSelector selector,
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.selector = selector;
    this.histogram = new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }

  @Override
  public void aggregate()
  {
    // In case of ExpressionColumnValueSelector isNull will compute the expression and then give the result,
    // the check for is NullHandling.replaceWithDefault is there to not have any performance impact of calling
    // isNull for default case.
    if (NullHandling.replaceWithDefault() || !selector.isNull()) {
      histogram.offer(selector.getFloat());
    }
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
  public double getDouble()
  {
    throw new UnsupportedOperationException("ApproximateHistogramAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
