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

import com.google.common.primitives.Longs;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Comparator;

public class FixedBucketsHistogramAggregator implements Aggregator
{
  private static final Logger LOG = new Logger(FixedBucketsHistogramAggregator.class);

  public static final String TYPE_NAME = "fixedBucketsHistogram";

  public static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((FixedBucketsHistogram) o).getCount(), ((FixedBucketsHistogram) o1).getCount());
    }
  };

  private final BaseObjectColumnValueSelector selector;

  private FixedBucketsHistogram histogram;

  public FixedBucketsHistogramAggregator(
      BaseObjectColumnValueSelector selector,
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode
  )
  {
    this.selector = selector;
    this.histogram = new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );
  }

  @Override
  public void aggregate()
  {
    Object val = selector.getObject();

    if (val == null) {
      if (NullHandling.replaceWithDefault()) {
        histogram.add(NullHandling.defaultDoubleValue());
      } else {
        histogram.incrementMissing();
      }
    } else if (val instanceof String) {
      LOG.info((String) val);
      histogram.combineHistogram(FixedBucketsHistogram.fromBase64((String) val));
    } else if (val instanceof FixedBucketsHistogram) {
      histogram.combineHistogram((FixedBucketsHistogram) val);
    } else if (val instanceof Number) {
      histogram.add(((Number) val).doubleValue());
    } else {
      throw new ISE("Unknown class for object: " + val.getClass());
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    return histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramAggregator does not support getDouble()");
  }

  @Override
  public boolean isNull()
  {
    return false;
  }

  @Override
  public void close()
  {

  }
}
